from oauth2client.service_account import ServiceAccountCredentials
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from pathlib import Path, PurePath
import asyncio
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
    def _build_local_tree(path: Path, match: str):
        for p in path.rglob(match):
            yield PurePath.relative_to(p, path)

    def _build_remote_tree(self, id: str, match: str):
        file = self.CreateFile({'id': id})
        file.FetchMetadata()

        tree = {}
        if self.is_folder(file):
            def recurse(file, path: Path):
                this_path = path / file['title']
                yield (this_path, file)
                if self.is_folder(file):
                    for inner in self.list_folder(file['id']):
                        yield from recurse(inner, this_path)

            tree = {path: f for inner in self.list_folder(file['id'])
                    for (path, f) in recurse(inner, Path())}
        else:
            tree = {Path(file['title']): file}

        # Filters for match pattern
        subtree = {k: v for k, v in tree.items() if k.match(match)}
        return subtree

    def download(self, local: Path, remote_id: str, remote_subfolder: Path, match: str):
        if not local.exists():
            raise ValueError('Destination path %s does not exist' % str(local))
        if not local.is_dir():
            raise ValueError(
                'Destination path %s must be a folder' % str(local))
        if not self.is_folder_id(remote_id):
            raise ValueError('Source id %s must be a folder' % str(remote_id))

        # Build remote tree of files / folder
        remote_tree = self._build_remote_tree(remote_id, match)

        # Filters for the requested subfolder
        filtered_subtree = {k.relative_to(remote_subfolder): v for k, v in remote_tree.items(
        ) if k.is_relative_to(remote_subfolder)}
        if not filtered_subtree:
            logging.info('No file found for subfolder %s',
                         str(remote_subfolder))

        # Download filtered tree
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def async_download():
            futures = []
            for path, file in sorted(filtered_subtree.items()):
                this_path = local / path
                if self.is_folder(file):
                    logging.info('Creating folder: %s, id: %s' %
                                 (path, file['id']))
                    this_path.mkdir(parents=True, exist_ok=True)
                else:
                    this_path.parent.mkdir(parents=True, exist_ok=True)
                    logging.info('Downloading file: %s, id: %s, mine: %s' % (
                        path, file['id'], file['mimeType']))
                    futures.append(loop.run_in_executor(
                        None, file.GetContentFile, this_path))
            [await f for f in futures]

        loop.run_until_complete(async_download())

    def upload(self, local: Path, remote_id: str, remote_subfolder: Path, match: str):
        if not local.exists():
            raise ValueError('Source path %s does not exist' % str(local))
        if not local.is_dir():
            raise ValueError('Source path %s must be a folder' % str(local))
        if not self.is_folder_id(remote_id):
            raise ValueError(
                'Destination id %s must be a folder' % str(remote_id))

        # Gather local data and remote data
        logging.info('Globing local path: %s.' % (local))
        local_tree = self._build_local_tree(local, match)
        remote_tree = self._build_remote_tree(remote_id, '*')

        def _create_file(path, is_dir):
            logging.info('Creating %s: %s' %
                         ('folder' if is_dir else 'file', str(path)))
            meta = {'title': str(path.name),
                    'parents': [{'id': remote_id if len(
                        path.parents) == 1 else remote_tree[path.parent]['id']}],
                    'mimeType': 'application/vnd.google-apps.folder' if is_dir else ''}
            file = self.CreateFile(meta)
            file.Upload()
            return file

        # Make sure remote subfolder exists
        def parents(path: Path):
            while len(path.parents):
                yield path
                path = path.parent

        for path in sorted(parents(remote_subfolder)):
            if not remote_tree.get(path, None):
                remote_tree[path] = _create_file(path, True)

        # Upload local tree
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def async_upload():
            futures = []
            for path in sorted(local_tree):
                local_path = local / path
                is_dir = local_path.is_dir()

                remote_path = remote_subfolder / path
                file = remote_tree.get(remote_path, None)
                if not file:
                    file = remote_tree[remote_path] = _create_file(
                        remote_path, is_dir)

                if not is_dir:
                    logging.info('Uploading file: %s, id: %s, mine: %s' % (
                        str(path), file['id'], file['mimeType']))
                    file.SetContentFile(str(local_path))
                    futures.append(
                        loop.run_in_executor(None, file.Upload))

            # Await completion
            [await f for f in futures]

        loop.run_until_complete(async_upload())


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

    parser.add_argument('direction', type=direction, help='upload/download')
    parser.add_argument('-l', '--local', default='',
                        help='Local directory path', required=True)
    parser.add_argument('-d', '--drive', default='',
                        help='Drive directory id.', required=True)
    parser.add_argument('-s', '--drive_subfolder', default='.',
                        help='Drive subfolder path.')
    parser.add_argument('-m', '--match', default='*',
                        help='Match paths against the provided pattern.')
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
        to_call(Path(args.local), args.drive, Path(
            args.drive_subfolder), args.match)
