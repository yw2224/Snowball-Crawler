import os
from lib.config import secretconf
from datetime import datetime, timedelta
from azure.storage.blob import AppendBlobService, BlockBlobService, ContentSettings, BlobPermissions, ContainerPermissions

textcontent = ContentSettings(content_type="text/plain;charset=utf-8")
blobpermission = BlobPermissions.READ
containerpermission = ContainerPermissions.READ

class AppendBlob:
    '''Append blob used to append data, each time called append, the content will be append to the blob'''
    def __init__(self):
        #account = account_name or secretconf["azure"]["storage"][0]["account"]
        #key = account_key or secretconf["azure"]["storage"][0]["key"]
        connstr = os.getenv("AZURE_STORAGE_CONNECTION_STRING", False) or secretconf["azure"]["storage"][0]["connection_string"]
        self.abservice = AppendBlobService(connection_string=connstr)
    
    def create(self, container, blob, metadata=None):
        '''Create an empty blob
        
        Args:
            container: name of the container
            blob: name of the blob, use '/' to create a folder
            metadata: meta data (dict object, value must be str) of the text

        Returns:
            url of blob
        '''
        self.abservice.create_blob(container, blob, metadata=metadata, content_settings = textcontent, if_none_match="*")

        now = datetime.now()
        start = now + timedelta(-1, 0, 0)
        expiry = now + timedelta(365, 0, 0)
        sastoken = self.abservice.generate_blob_shared_access_signature(container, blob, permission=blobpermission, expiry=expiry, start=start)

        return self.abservice.make_blob_url(container, blob, sas_token=sastoken)

    def appendText(self, container, blob, text, metadata=None):
        '''Append text to blob'''
        self.abservice.append_blob_from_text(container, blob, text)

class BlockBlob:
    '''Block blob is used to transfer big data, it cannot append data, same blob will be overwrite'''
    def __init__(self, is_emulated=False, account_name=None, account_key=None):
        '''
        Args:
            is_emulated: is running under umulated env
            account_name: storageaccount of azure storage, will get from environment variables by key "AZURE_STORAGE_ACCOUNT"
            account_key:  storageaccesskey of azure storage, will get from environment variables by key "AZURE_STORAGE_ACCESS_KEY"
        '''

        # account = account_name or secretconf["azure"]["storage"][0]["account"]
        # key = account_key or secretconf["azure"]["storage"][0]["key"]
        connstr = os.getenv("AZURE_STORAGE_CONNECTION_STRING", False) or secretconf["azure"]["storage"][0]["connection_string"]
        self.bbservice = BlockBlobService(connection_string=connstr, is_emulated=is_emulated)

    def writeText(self, container, blob, text, metadata=None):
        '''Write text content to blob

        Args:
            container:  Name of the container
            blob:   Name of the blob
            text: text content to write
            metadata: meta data (dict object) of the text

        Returns:
            url of this blob

        Examples:
            b = Blob()
            url = b.writeText("mycontainer", "myblob", "my text", {"author": "me"})

        NOTE:\n
            container:
                    1. Container names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
                    2. Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive dashes are not permitted in container names.
                    3. All letters in a container name must be lowercase.
                    4. Container names must be from 3 through 63 characters long.
            blob:
                    1. A blob name can contain any combination of characters.
                    2. A blob name must be at least one character long and cannot be more than 1,024 characters long.
                    3. Blob names are case-sensitive.
                    4. Reserved URL characters must be properly escaped.
                    5. The number of path segments comprising the blob name cannot exceed 254. A path segment is the string between consecutive delimiter characters (e.g., the forward slash '/') that corresponds to the name of a virtual directory.

        '''

        # TODO: how to make it shorter?
        textfunc = lambda c, b, t, metadata=None, content_settings=None: self.bbservice.create_blob_from_text(
            c, b, t, encoding="utf-8", metadata=metadata, content_settings=content_settings)
        return self.__writecontent(container, blob, text, textfunc, metadata=metadata, content_settings=textcontent)

    def readText(self, container, blob):
        '''Read text of blob
        
        Args:
            container: name of container
            blob: name of blob

        Returns:
            text content and metadata

        Examples:
            b = Blob()
            text, metadata = b.readText("mycontainer", "myblob")
        '''
        b = self.bbservice.get_blob_to_text(container, blob)
        return (b.content, b.metadata)

    def writeStream(self, container, blob, stream, metadata=None):
        '''Write byte stream to blob
        
        Args:
            container: name of container
            blob: name of blob
            stream: ByteIO stream to save
            metadata: metadata to save

        Returns:
            url of the new blob

        Examples:
            b = Blob()
            with open("/path/to/binary", mode="rb") f:
                b.writeStream("mycontainer", "myblob", f, metadata={"author":"me"})
        '''
        return self.__writecontent(container, blob, stream, self.bbservice.create_blob_from_stream, metadata=metadata)

    def readStream(self, container, blob, outstream):
        '''read the binary content from blob to output stream
        
        Args:
            container: name of the container
            blob: name of the blob
            outstream: output of stream content

        Returns:
            length of stream, and metadata

        Examples:
            b = Blob()
            outstream = io.BytesIO()
            l, m = b.readStream("mycontainer", "myblob", outstream)
        '''
        b = self.bbservice.get_blob_to_stream(container, blob, outstream)

        return (b.properties.content_length, b.metadata)

    def __writecontent(self, container, blob, content, writefunc, metadata=None, content_settings=None):
        now = datetime.now()
        start = now + timedelta(-1, 0, 0)
        expiry = now + timedelta(365, 0, 0)

        if not self.bbservice.exists(container):
            self.bbservice.create_container(container)

        writefunc(container, blob, content, metadata=metadata,content_settings=content_settings)
        sastoken = self.bbservice.generate_blob_shared_access_signature(container, blob, permission=blobpermission, expiry=expiry, start=start)

        return self.bbservice.make_blob_url(container, blob, sas_token=sastoken)
