import os
from datetime import datetime
from urllib.parse import urljoin

import pyarrow
import os
from datetime import datetime
from urllib.parse import urljoin

from django.conf import settings
from django.core.exceptions import SuspiciousFileOperation
from django.core.files import File, locks
from django.core.files.move import file_move_safe
from django.core.signals import setting_changed
from django.utils import timezone
from django.utils._os import safe_join
from django.utils.crypto import get_random_string
from django.utils.deconstruct import deconstructible
from django.utils.encoding import filepath_to_uri
from django.utils.functional import LazyObject, cached_property
from django.utils.module_loading import import_string
from django.utils.text import get_valid_filename
from django.conf import settings
from django.core.files import File
from django.core.files.storage import Storage
from django.core.signals import setting_changed
from django.utils import timezone
from django.utils._os import safe_join
from django.utils.deconstruct import deconstructible
from django.utils.encoding import filepath_to_uri
from django.utils.functional import cached_property
from django.utils.timezone import now
from unidecode import unidecode
import subprocess



# import mimetypes
# import warnings
# from datetime import timedelta
# from tempfile import SpooledTemporaryFile

# from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation
# from django.core.files.base import File
# from django.utils import timezone
# # from django.utils.deconstruct import deconstructible
# from django.utils.encoding import force_bytes

# from storages.base import BaseStorage
# from storages.utils import (
#     check_location, clean_name, get_available_overwrite_name, safe_join,
#     setting,
# )

# try:
#     from google.cloud.exceptions import NotFound
#     from google.cloud.storage import Blob, Client
#     from google.cloud.storage.blob import _quote
# except ImportError:
#     raise ImproperlyConfigured("Could not load Google Cloud Storage bindings.\n"
#                                "See https://github.com/GoogleCloudPlatform/gcloud-python")

# def clean_char(char, replace=''):
#     """
#     Remove ['.',',','@'] and accents to char
#     :param char: char
#     :return: clean char
#     """
#     bad_tokens = [",", ";", ".",
#                   "!", "'", ".",
#                   "-", '"', "@",
#                   r"\n", r"\r", '?',
#                   '_']
#     for delete in bad_tokens:
#         char = char.replace(delete, replace)
#     return char


# CONTENT_TYPE = 'content_type'


# class GoogleCloudFile(File):
#     def __init__(self, name, mode, storage):
#         self.name = name
#         self.mime_type = mimetypes.guess_type(name)[0]
#         self._mode = mode
#         self._storage = storage
#         self.blob = storage.bucket.get_blob(name)
#         if not self.blob and 'w' in mode:
#             self.blob = Blob(
#                 self.name, storage.bucket,
#                 chunk_size=storage.blob_chunk_size)
#         self._file = None
#         self._is_dirty = False

#     @property
#     def size(self):
#         return self.blob.size

#     def _get_file(self):
#         if self._file is None:
#             self._file = SpooledTemporaryFile(
#                 max_size=self._storage.max_memory_size,
#                 suffix=".GSStorageFile",
#                 dir=setting("FILE_UPLOAD_TEMP_DIR")
#             )
#             if 'r' in self._mode:
#                 self._is_dirty = False
#                 self.blob.download_to_file(self._file)
#                 self._file.seek(0)
#         return self._file

#     def _set_file(self, value):
#         self._file = value

#     file = property(_get_file, _set_file)

#     def read(self, num_bytes=None):
#         if 'r' not in self._mode:
#             raise AttributeError("File was not opened in read mode.")

#         if num_bytes is None:
#             num_bytes = -1

#         return super().read(num_bytes)

#     def write(self, content):
#         if 'w' not in self._mode:
#             raise AttributeError("File was not opened in write mode.")
#         self._is_dirty = True
#         return super().write(force_bytes(content))

#     def close(self):
#         if self._file is not None:
#             if self._is_dirty:
#                 blob_params = self._storage.get_object_parameters(self.name)
#                 self.blob.upload_from_file(
#                     self.file, rewind=True, content_type=self.mime_type,
#                     predefined_acl=blob_params.get('acl', self._storage.default_acl))
#             self._file.close()
#             self._file = None


# @deconstructible
# # class GoogleCloudStorage(BaseStorage):
# class HadoopStorage(BaseStorage):
#     hadoop_host: str = getattr(settings, 'HADOOP_HOST', 'localhost')
#     hadoop_port: int = getattr(settings, 'HADOOP_PORT', 8020)
#     hadoop_user: str = getattr(settings, 'HADOOP_USER', 'hadoop')
#     hadoop_home: str = getattr(settings, 'HADOOP_HOME', '/usr/lib/hadoop/')
#     replications: int = 3    
#     def __init__(self, **settings):
#         super().__init__(**settings)

#         check_location(self)        
#         os.environ['HADOOP_HOME'] = self.hadoop_home
#         hadoop_bin = os.path.normpath(os.environ['HADOOP_HOME'])  +"/bin/"  #'{0}/bin/hadoop'.format(os.environ['HADOOP_HOME'])
#         os.chdir(hadoop_bin)
#         hadoop_bin_exe = os.path.join(hadoop_bin, 'hadoop.cmd')
#         print(hadoop_bin_exe)
#         classpath = subprocess.check_output([hadoop_bin_exe, 'classpath', '--glob'])
#         os.environ['CLASSPATH'] = classpath.decode('utf-8')      

#         self.hdfs = pyarrow.hdfs.connect(host=self.hadoop_host,port=self.hadoop_port)
#         self._bucket = None
#         self._client = None

#     def get_default_settings(self):
#         return {
#             "project_id": setting('GS_PROJECT_ID'),
#             "credentials": setting('GS_CREDENTIALS'),
#             "bucket_name": setting('GS_BUCKET_NAME'),
#             "custom_endpoint": setting('GS_CUSTOM_ENDPOINT', None),
#             "location": setting('GS_LOCATION', ''),
#             "default_acl": setting('GS_DEFAULT_ACL'),
#             "querystring_auth": setting('GS_QUERYSTRING_AUTH', True),
#             "expiration": setting('GS_EXPIRATION', timedelta(seconds=86400)),
#             "file_overwrite": setting('GS_FILE_OVERWRITE', True),
#             "cache_control": setting('GS_CACHE_CONTROL'),
#             "object_parameters": setting('GS_OBJECT_PARAMETERS', {}),
#             # The max amount of memory a returned file can take up before being
#             # rolled over into a temporary file on disk. Default is 0: Do not
#             # roll over.
#             "max_memory_size": setting('GS_MAX_MEMORY_SIZE', 0),
#             "blob_chunk_size": setting('GS_BLOB_CHUNK_SIZE'),
#         }

#     @property
#     def client(self):
#         if self._client is None:
#             self._client = Client(
#                 project=self.project_id,
#                 credentials=self.credentials
#             )
#         return self._client

#     @property
#     def bucket(self):
#         if self._bucket is None:
#             self._bucket = self.client.bucket(self.bucket_name)
#         return self._bucket

#     def _normalize_name(self, name):
#         """
#         Normalizes the name so that paths like /path/to/ignored/../something.txt
#         and ./file.txt work.  Note that clean_name adds ./ to some paths so
#         they need to be fixed here. We check to make sure that the path pointed
#         to is not outside the directory specified by the LOCATION setting.
#         """
#         try:
#             return safe_join(self.location, name)
#         except ValueError:
#             raise SuspiciousOperation("Attempted access to '%s' denied." %
#                                       name)

#     def _open(self, name, mode='rb'):
#         name = self._normalize_name(clean_name(name))
#         file_object = GoogleCloudFile(name, mode, self)
#         if not file_object.blob:
#             raise FileNotFoundError('File does not exist: %s' % name)
#         return file_object

#     def _save(self, name, content):
#         cleaned_name = clean_name(name)
#         name = self._normalize_name(cleaned_name)

#         content.name = cleaned_name
#         file_object = GoogleCloudFile(name, 'rw', self)

#         upload_params = {}
#         blob_params = self.get_object_parameters(name)
#         upload_params['predefined_acl'] = blob_params.pop('acl', self.default_acl)

#         if CONTENT_TYPE not in blob_params:
#             upload_params[CONTENT_TYPE] = file_object.mime_type

#         for prop, val in blob_params.items():
#             setattr(file_object.blob, prop, val)

#         file_object.blob.upload_from_file(content, rewind=True, size=content.size, **upload_params)
#         return cleaned_name

#     def get_object_parameters(self, name):
#         """Override this to return a dictionary of overwritable blob-property to value.

#         Returns GS_OBJECT_PARAMETRS by default. See the docs for all possible options.
#         """
#         object_parameters = self.object_parameters.copy()

#         if 'cache_control' not in object_parameters and self.cache_control:
#             warnings.warn(
#                 'The GS_CACHE_CONTROL setting is deprecated. Use GS_OBJECT_PARAMETERS to set any '
#                 'writable blob property or override GoogleCloudStorage.get_object_parameters to '
#                 'vary the parameters per object.', DeprecationWarning
#             )
#             object_parameters['cache_control'] = self.cache_control

#         return object_parameters

#     def delete(self, name):
#         name = self._normalize_name(clean_name(name))
#         try:
#             self.bucket.delete_blob(name)
#         except NotFound:
#             pass

#     def exists(self, name):
#         if not name:  # root element aka the bucket
#             try:
#                 self.client.get_bucket(self.bucket)
#                 return True
#             except NotFound:
#                 return False

#         name = self._normalize_name(clean_name(name))
#         return bool(self.bucket.get_blob(name))

#     def listdir(self, name):
#         name = self._normalize_name(clean_name(name))
#         # For bucket.list_blobs and logic below name needs to end in /
#         # but for the root path "" we leave it as an empty string
#         if name and not name.endswith('/'):
#             name += '/'

#         iterator = self.bucket.list_blobs(prefix=name, delimiter='/')
#         blobs = list(iterator)
#         prefixes = iterator.prefixes

#         files = []
#         dirs = []

#         for blob in blobs:
#             parts = blob.name.split("/")
#             files.append(parts[-1])
#         for folder_path in prefixes:
#             parts = folder_path.split("/")
#             dirs.append(parts[-2])

#         return list(dirs), files

#     def _get_blob(self, name):
#         # Wrap google.cloud.storage's blob to raise if the file doesn't exist
#         blob = self.bucket.get_blob(name)

#         if blob is None:
#             raise NotFound('File does not exist: {}'.format(name))

#         return blob

#     def size(self, name):
#         name = self._normalize_name(clean_name(name))
#         blob = self._get_blob(name)
#         return blob.size

#     def modified_time(self, name):
#         name = self._normalize_name(clean_name(name))
#         blob = self._get_blob(name)
#         return timezone.make_naive(blob.updated)

#     def get_modified_time(self, name):
#         name = self._normalize_name(clean_name(name))
#         blob = self._get_blob(name)
#         updated = blob.updated
#         return updated if setting('USE_TZ') else timezone.make_naive(updated)

#     def get_created_time(self, name):
#         """
#         Return the creation time (as a datetime) of the file specified by name.
#         The datetime will be timezone-aware if USE_TZ=True.
#         """
#         name = self._normalize_name(clean_name(name))
#         blob = self._get_blob(name)
#         created = blob.time_created
#         return created if setting('USE_TZ') else timezone.make_naive(created)

#     def url(self, name):
#         """
#         Return public url or a signed url for the Blob.
#         This DOES NOT check for existance of Blob - that makes codes too slow
#         for many use cases.
#         """
#         name = self._normalize_name(clean_name(name))
#         blob = self.bucket.blob(name)
#         blob_params = self.get_object_parameters(name)
#         no_signed_url = (
#             blob_params.get('acl', self.default_acl) == 'publicRead' or not self.querystring_auth)

#         if not self.custom_endpoint and no_signed_url:
#             return blob.public_url
#         elif no_signed_url:
#             return '{storage_base_url}/{quoted_name}'.format(
#                 storage_base_url=self.custom_endpoint,
#                 quoted_name=_quote(name, safe=b"/~"),
#             )
#         elif not self.custom_endpoint:
#             return blob.generate_signed_url(
#                 expiration=self.expiration, version="v4"
#             )
#         else:
#             return blob.generate_signed_url(
#                 bucket_bound_hostname=self.custom_endpoint,
#                 expiration=self.expiration,
#                 version="v4",
#             )

#     def get_available_name(self, name, max_length=None):
#         name = clean_name(name)
#         if self.file_overwrite:
#             return get_available_overwrite_name(name, max_length)
        #return super().get_available_name(name, max_length)

@deconstructible
class HadoopStorage(Storage):
    hadoop_host: str = getattr(settings, 'HADOOP_HOST', 'localhost')
    hadoop_port: int = getattr(settings, 'HADOOP_PORT', 8020)
    hadoop_user: str = getattr(settings, 'HADOOP_USER', 'hadoop')
    hadoop_home: str = getattr(settings, 'HADOOP_HOME', '/usr/lib/hadoop/')
    replications: int = 3

    def __init__(self, location=None, base_url=None, file_permissions_mode=None,
                 directory_permissions_mode=None):
        self._location = location
        self._base_url = base_url
        self._file_permissions_mode = file_permissions_mode
        self._directory_permissions_mode = directory_permissions_mode

        # # e['ARROW_LIBHDFS_DIR'] = '/opt/hadoop/hadoop/lib/native/'
        # os.environ['HADOOP_HOME'] = self.hadoop_home
        # hadoop_bin = os.path.normpath(os.environ['HADOOP_HOME'])  +"/bin/"  #'{0}/bin/hadoop'.format(os.environ['HADOOP_HOME'])
        # # e['JAVA_HOME'] = "/usr/lib/jvm/java-11-openjdk-amd64"
        # # os.environ['CLASSPATH'] = f'{self.hadoop_home}/bin/hdfs classpath --glob'
        # os.chdir(hadoop_bin)
        # hadoop_bin_exe = os.path.join(hadoop_bin, 'hadoop.cmd')
        # print(hadoop_bin_exe)
        # classpath = subprocess.check_output([hadoop_bin_exe, 'classpath', '--glob'])
        # os.environ['CLASSPATH'] = classpath.decode('utf-8')      

        self.hdfs = pyarrow.hdfs.connect(host=self.hadoop_host,port=self.hadoop_port)
            # self.hadoop_host, self.hadoop_port, user=self.hadoop_user,
            # extra_conf={'dfs.replication': str(self.replications)})
        setting_changed.connect(self._clear_cached_properties) 

    def __init__(self, location=None, base_url=None, file_permissions_mode=None,
                 directory_permissions_mode=None):
        self._location = location
        self._base_url = base_url
        self._file_permissions_mode = file_permissions_mode
        self._directory_permissions_mode = directory_permissions_mode
        setting_changed.connect(self._clear_cached_properties)

    def _clear_cached_properties(self, setting, **kwargs):
        """Reset setting based property values."""
        if setting == 'MEDIA_ROOT':
            self.__dict__.pop('base_location', None)
            self.__dict__.pop('location', None)
        elif setting == 'MEDIA_URL':
            self.__dict__.pop('base_url', None)
        elif setting == 'FILE_UPLOAD_PERMISSIONS':
            self.__dict__.pop('file_permissions_mode', None)
        elif setting == 'FILE_UPLOAD_DIRECTORY_PERMISSIONS':
            self.__dict__.pop('directory_permissions_mode', None)

    def _value_or_setting(self, value, setting):
        return setting if value is None else value

    @cached_property
    def base_location(self):
        return self._value_or_setting(self._location, settings.MEDIA_ROOT)

    @cached_property
    def location(self):
        return os.path.abspath(self.base_location)

    @cached_property
    def base_url(self):
        if self._base_url is not None and not self._base_url.endswith('/'):
            self._base_url += '/'
        return self._value_or_setting(self._base_url, settings.MEDIA_URL)

    @cached_property
    def file_permissions_mode(self):
        return self._value_or_setting(self._file_permissions_mode, settings.FILE_UPLOAD_PERMISSIONS)

    @cached_property
    def directory_permissions_mode(self):
        return self._value_or_setting(self._directory_permissions_mode, settings.FILE_UPLOAD_DIRECTORY_PERMISSIONS)

    def _open(self, name, mode='rb'):
        return File(open(self.path(name), mode))

    def _save(self, name, content):
        full_path = self.path(name)

        # Create any intermediate directories that do not exist.
        directory = os.path.dirname(full_path)
        if not os.path.exists(directory):
            try:
                if self.directory_permissions_mode is not None:
                    # os.makedirs applies the global umask, so we reset it,
                    # for consistency with file_permissions_mode behavior.
                    old_umask = os.umask(0)
                    try:
                        os.makedirs(directory, self.directory_permissions_mode)
                    finally:
                        os.umask(old_umask)
                else:
                    os.makedirs(directory)
            except FileExistsError:
                # There's a race between os.path.exists() and os.makedirs().
                # If os.makedirs() fails with FileExistsError, the directory
                # was created concurrently.
                pass
        if not os.path.isdir(directory):
            raise IOError("%s exists and is not a directory." % directory)

        # There's a potential race condition between get_available_name and
        # saving the file; it's possible that two threads might return the
        # same name, at which point all sorts of fun happens. So we need to
        # try to create the file, but if it already exists we have to go back
        # to get_available_name() and try again.

        while True:
            try:
                # This file has a file path that we can move.
                if hasattr(content, 'temporary_file_path'):
                    file_move_safe(content.temporary_file_path(), full_path)

                # This is a normal uploadedfile that we can stream.
                else:
                    # This fun binary flag incantation makes os.open throw an
                    # OSError if the file already exists before we open it.
                    flags = (os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                             getattr(os, 'O_BINARY', 0))
                    # The current umask value is masked out by os.open!
                    fd = os.open(full_path, flags, 0o666)
                    _file = None
                    try:
                        locks.lock(fd, locks.LOCK_EX)
                        for chunk in content.chunks():
                            if _file is None:
                                mode = 'wb' if isinstance(chunk, bytes) else 'wt'
                                _file = os.fdopen(fd, mode)
                            _file.write(chunk)
                    finally:
                        locks.unlock(fd)
                        if _file is not None:
                            _file.close()
                        else:
                            os.close(fd)
            except FileExistsError:
                # A new name is needed if the file exists.
                name = self.get_available_name(name)
                full_path = self.path(name)
            else:
                # OK, the file save worked. Break out of the loop.
                break

        if self.file_permissions_mode is not None:
            os.chmod(full_path, self.file_permissions_mode)

        # Store filenames with forward slashes, even on Windows.
        return name.replace('\\', '/')

    def delete(self, name):
        assert name, "The name argument is not allowed to be empty."
        name = self.path(name)
        # If the file or directory exists, delete it from the filesystem.
        try:
            if os.path.isdir(name):
                os.rmdir(name)
            else:
                os.remove(name)
        except FileNotFoundError:
            # FileNotFoundError is raised if the file or directory was removed
            # concurrently.
            pass

    def exists(self, name):
        return os.path.exists(self.path(name))

    def listdir(self, path):
        path = self.path(path)
        directories, files = [], []
        for entry in os.listdir(path):
            if os.path.isdir(os.path.join(path, entry)):
                directories.append(entry)
            else:
                files.append(entry)
        return directories, files

    def path(self, name):
        return safe_join(self.location, name)

    def size(self, name):
        return os.path.getsize(self.path(name))

    def url(self, name):
        if self.base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        url = filepath_to_uri(name)
        if url is not None:
            url = url.lstrip('/')
        return urljoin(self.base_url, url)

    def _datetime_from_timestamp(self, ts):
        """
        If timezone support is enabled, make an aware datetime object in UTC;
        otherwise make a naive one in the local timezone.
        """
        if settings.USE_TZ:
            # Safe to use .replace() because UTC doesn't have DST
            return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
        else:
            return datetime.fromtimestamp(ts)

    def get_accessed_time(self, name):
        return self._datetime_from_timestamp(os.path.getatime(self.path(name)))

    def get_created_time(self, name):
        return self._datetime_from_timestamp(os.path.getctime(self.path(name)))

    def get_modified_time(self, name):
        return self._datetime_from_timestamp(os.path.getmtime(self.path(name)))


def get_storage_class(import_path=None):
    return import_string(import_path or settings.DEFAULT_FILE_STORAGE)
