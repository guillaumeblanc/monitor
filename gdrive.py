from oauth2client.service_account import ServiceAccountCredentials
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from pathlib import Path
from pathlib import PurePath
import argparse
import logging
import base64
import json


class GoogleDriveClient(GoogleDrive):
    def __init__(self, auth):
        return super().__init__(auth=auth)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    @staticmethod
    def is_folder(file):
        return file['mimeType'] == 'application/vnd.google-apps.folder'

    def is_folder_id(self, id):
        file = self.CreateFile({'id': id})
        file.FetchMetadata()
        return self.is_folder(file)

    def list_folder(self, id: str):
        for file_list in self.ListFile({'q': "'%s' in parents and trashed=false" % id, 'maxResults': 100}):
            for file in file_list:
                yield file

    @staticmethod
    def _build_local_tree(path: Path):
        for p in path.rglob("*"):
            yield PurePath.relative_to(p, path)

    def _build_remote_tree(self, id: str):
        file = self.CreateFile({'id': id})
        file.FetchMetadata()

        def recurse(file, path: Path):
            this_path = path / file['title']
            yield (this_path, file)
            if self.is_folder(file):
                for inner in self.list_folder(file['id']):
                    yield from recurse(inner, this_path)

        if self.is_folder(file):
            return {path: f for inner in self.list_folder(file['id'])
                    for (path, f) in recurse(inner, Path())}
        else:
            return {Path(file['title']): file}

    def download(self, local: Path, remote_id: str):
        if not local.exists():
            raise ValueError('Destination path %s does not exist' % str(local))
        if not local.is_dir():
            raise ValueError(
                'Destination path %s must be a folder' % str(dest))
        if not self.is_folder_id(remote_id):
            raise ValueError('Source id %s must be a folder' % str(remote_id))

        remote_tree = self._build_remote_tree(remote_id)
        for path, file in sorted(remote_tree.items()):
            this_path = local / path
            if self.is_folder(file):
                logging.info('Creating folder: %s, id: %s' %
                             (path, file['id']))
                this_path.mkdir(parents=True, exist_ok=True)
            else:
                logging.info('Downloading file: %s, id: %s, mine: %s' % (
                    path, file['id'], file['mimeType']))
                file.GetContentFile(this_path)

    def upload(self, local: Path, remote_id: str):
        if not local.exists():
            raise ValueError('Source path %s does not exist' % str(local))
        if not local.is_dir():
            raise ValueError('Source path %s must be a folder' % str(local))
        if not self.is_folder_id(remote_id):
            raise ValueError(
                'Destination id %s must be a folder' % str(remote_id))

        local_tree = self._build_local_tree(local)
        remote_tree = self._build_remote_tree(remote_id)

        def _create_file(path, is_dir):
            logging.info('Creating %s: %s' %
                         ('folder' if is_dir else 'file', str(path)))
            meta = {'title': str(path.name),
                    'parents': [{'id': remote_id if len(
                        path.parents) == 1 else remote_tree[path.parents[0]]['id']}],
                    'mimeType': 'application/vnd.google-apps.folder' if is_dir else ''}
            file = self.CreateFile(meta)
            file.Upload()
            return file

        for path in sorted(local_tree):
            full_path = local / path
            is_dir = full_path.is_dir()

            file = remote_tree.get(path, None)
            if not file:
                file = remote_tree[path] = _create_file(path, is_dir)

            if not is_dir:
                logging.info('Uploading file: %s, id: %s, mine: %s' % (
                    str(path), file['id'], file['mimeType']))
                file.SetContentFile(str(full_path))
                file.Upload()


def authenticate(key):
    auth = GoogleAuth()
    scope = ["https://www.googleapis.com/auth/drive"]
    auth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        key, scope)
    return auth


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    def direction(astring):
        if not astring in ['upload', 'download']:
            raise ValueError
        return astring

    parser.add_argument('direction', type=direction, help='upload/dowload')
    parser.add_argument('-l', '--local', default='',
                        help='Local directory path', required=True)
    parser.add_argument('-d', '--drive', default='',
                        help='Drive directory id.', required=True)
    parser.add_argument('-c', '--credentials', default='',
                        help='Google Service Account credentials as base64 string.', required=True)
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    key = json.loads(base64.b64decode(args.credentials))
    auth = authenticate(key)
    with GoogleDriveClient(auth) as drive:
        to_call = drive.download if args.direction == 'download' else drive.upload
        to_call(Path(args.local), args.drive)
