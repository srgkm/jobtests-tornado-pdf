import datetime
import logging
import pathlib
import sqlite3
import uuid

import tornado.auth
import tornado.concurrent
import tornado.escape
import tornado.gen
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options
from wand.image import Image


class DB:
    name = 'main.db'
    conn = None

    def __init__(self, name=None):
        if name:
            self.name = name
        self.conn = sqlite3.connect(self.name)

    def create_file_table(self):
        q = """
        CREATE TABLE IF NOT EXISTS file (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            file TEXT,
            file_preview_counter INTEGER DEFAULT 0,
            file_preview_generated BOOLEAN DEFAULT FALSE,
            user_id INTEGER,
            user_name TEXT,
            created_time DATETIME
        );
        """
        return self.conn.execute(q)

    def insert_file(self, **kwargs):
        fields = ', '.join([k for k in kwargs])
        placeholders = ', '.join([':%s' % k for k in kwargs])
        q = 'INSERT INTO file (%s) VALUES (%s);' % (fields, placeholders)
        return self.conn.execute(q, kwargs)

    def update_file(self, **kwargs):
        q = """
        UPDATE file
        SET file_preview_counter = :file_preview_counter, file_preview_generated = :file_preview_generated 
        WHERE id = :file_id;
        """
        return self.conn.execute(q, kwargs)

    def list_files(self):
        q = 'SELECT * FROM file ORDER BY created_time ASC;'
        return self.conn.execute(q)


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        user = self.get_secure_cookie(self.settings['cookie_name'])
        if user:
            return tornado.escape.json_decode(user)


class MainHandler(BaseHandler):
    def get(self):
        self.render('main.html', user=self.current_user)


executor = tornado.concurrent.futures.ThreadPoolExecutor(8)


@tornado.gen.coroutine
def generate_file_previews(*args):
    yield executor.submit(_generate_file_previews, *args)


def _generate_file_previews(file_id, file_path):
    file_preview_counter = 0
    pages = Image(filename=file_path, resolution=100)
    for i, page in enumerate(pages.sequence):
        with Image(page) as preview:
            preview.format = 'png'
            preview.save(filename='%s.%s.png' % (file_path, i))
        file_preview_counter += 1
    db = DB()
    db.update_file(
        file_id=file_id,
        file_preview_counter=file_preview_counter,
        file_preview_generated=True
    )
    db.conn.commit()
    logging.info('File preview generated: %s' % file_id)


class FilesHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        db = DB()
        db.conn.row_factory = sqlite3.Row
        files = [
            file for file in db.list_files()
        ]
        self.render('_files.html', files=files)

    @tornado.web.authenticated
    @tornado.gen.coroutine
    def post(self):
        db = DB()
        user_id = self.current_user['id']
        user_name = self.current_user['name']
        ok_file_counter = 0
        for _field_name, _files in self.request.files.items():
            for _file in _files:
                # Check user file
                # TODO: Check body length?
                if not _file['content_type'] == 'application/pdf':
                    continue
                if not _file['filename'].endswith('pdf'):
                    continue
                # Create user subdir
                user_dir = pathlib.Path(self.settings['uploads_dir'], user_id)
                if not user_dir.exists():
                    user_dir.mkdir()
                # Save user file
                file_path = user_dir.joinpath('%s.pdf' % str(uuid.uuid4()))
                with open(str(file_path), 'wb') as file_obj:
                    file_obj.write(_file['body'])
                res = db.insert_file(
                    file=str(file_path),
                    user_id=user_id,
                    user_name=user_name,
                    created_time=datetime.datetime.now()
                )
                db.conn.commit()
                # Offload file previews gen
                tornado.ioloop.IOLoop.current().spawn_callback(
                    generate_file_previews,
                    res.lastrowid,
                    str(file_path)
                )
                ok_file_counter += 1
        # TODO: Do we need fire self.flush?
        self.set_header('Content-Type', 'application/json')
        self.write(tornado.escape.json_encode({'ok_file_counter': ok_file_counter}))


class LoginHandler(BaseHandler, tornado.auth.FacebookGraphMixin):
    @tornado.gen.coroutine
    def get(self):
        absolute_url = self.request.protocol + "://" + self.request.host + self.settings['login_url']
        if self.get_argument('code', False):
            fb_user = yield self.get_authenticated_user(
                redirect_uri=absolute_url,
                client_id=self.settings['facebook_api_key'],
                client_secret=self.settings['facebook_secret'],
                code=self.get_argument('code'),
            )
            if not fb_user:
                raise tornado.web.HTTPError(500, 'Facebook Auth Failed')
            self.set_secure_cookie(
                name=self.settings['cookie_name'],
                value=tornado.escape.json_encode(fb_user),
            )
            self.redirect('/')
        else:
            yield self.authorize_redirect(
                redirect_uri=absolute_url,
                client_id=self.settings['facebook_api_key'],
            )


class LogoutHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        self.clear_cookie(self.settings['cookie_name'])
        self.redirect('/')


def main():
    define('port', default=8888, help='', type=int)
    define('facebook_api_key', help='', type=str)
    define('facebook_secret', help='', type=str)

    tornado.options.parse_command_line()

    settings = {
        'debug': True,
        'cookie_secret': 'megasecs',
        'cookie_name': 'fbu',
        # 'xsrf_cookies': True,
        'login_url': '/login/',
        'facebook_api_key': options.facebook_api_key,
        'facebook_secret': options.facebook_secret,
        'uploads_dir': 'uploads',
    }

    handlers = [
        (r'/', MainHandler),
        (r'/files/', FilesHandler),
        (r'/login/', LoginHandler),
        (r'/logout/', LogoutHandler),
        (r'/uploads/(.*)', tornado.web.StaticFileHandler, {'path': settings['uploads_dir']}),
    ]

    # Create table
    db = DB()
    db.create_file_table()

    # Create uploads dir
    upload_path = pathlib.Path(settings['uploads_dir'])
    if not upload_path.exists():
        upload_path.mkdir()

    app = tornado.web.Application(
        handlers,
        **settings
    )

    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)

    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
