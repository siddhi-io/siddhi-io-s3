# API Docs - v1.0.5

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## S3

### copy *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Copy a file within Amazon AWS S3 buckets.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class, <STRING> aws.access.key, <STRING> aws.secret.key)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class, <STRING> aws.access.key, <STRING> aws.secret.key, <STRING> versioning.enabled)
s3:copy(<STRING> from.bucket.name, <STRING> from.key, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class, <STRING> aws.access.key, <STRING> aws.secret.key, <STRING> versioning.enabled, <STRING> bucket.acl)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">from.bucket.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Name of the S3 bucket which is copying from</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">from.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Key of the object to be copied</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Name of the destination S3 bucket</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Key of the destination object</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">async</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Toggle async mode</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">credential.provider.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS credential provider class to be used. If blank along with the username and the password, default credential provider will be used.</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.region</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The region to be used to create the bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">storage.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS storage class for the destination object</p></td>
        <td style="vertical-align: top">standard</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.access.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS access key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.secret.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS secret key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">versioning.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Flag to enable versioning support in the destination bucket</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.acl</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Access control list for the destination bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from FooStream#s3:copy('stock-source-bucket', 'stocks.txt', 'stock-backup-bucket', '/backup/stocks.txt')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Copy object from one bucket to another.</p>
<p></p>
### delete *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Delete an object from an Amazon AWS S3 bucket</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
s3:delete(<STRING> bucket.name, <STRING> key)
s3:delete(<STRING> bucket.name, <STRING> key, <BOOL> async)
s3:delete(<STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class)
s3:delete(<STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region)
s3:delete(<STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> aws.access.key, <STRING> aws.secret.key)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Name of the S3 bucket</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Key of the object</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">async</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Toggle async mode</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">credential.provider.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS credential provider class to be used. If blank along with the username and the password, default credential provider will be used.</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.region</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The region to be used to create the bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.access.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS access key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.secret.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS secret key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from FooStream#s3:delete('s3-file-bucket', '/uploads/stocks.txt')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Delete the object at '/uploads/stocks.txt' from the bucket.</p>
<p></p>
### uploadFile *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Uploads a file to an Amazon AWS S3 bucket</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class, <STRING> aws.access.key, <STRING> aws.secret.key)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class, <STRING> aws.access.key, <STRING> aws.secret.key, <STRING> versioning.enabled)
s3:uploadFile(<STRING> file.path, <STRING> bucket.name, <STRING> key, <BOOL> async, <STRING> credential.provider.class, <STRING> aws.region, <STRING> storage.class, <STRING> aws.access.key, <STRING> aws.secret.key, <STRING> versioning.enabled, <STRING> bucket.acl)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">file.path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Path of the file to be uploaded</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Name of the S3 bucket</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Key of the object</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">async</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Toggle async mode</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">credential.provider.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS credential provider class to be used. If blank along with the username and the password, default credential provider will be used.</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.region</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The region to be used to create the bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">storage.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS storage class</p></td>
        <td style="vertical-align: top">standard</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.access.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS access key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.secret.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS secret key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">versioning.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Flag to enable versioning support in the bucket</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.acl</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Access control list for the bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from FooStream#s3:upload('/Users/wso2/files/stocks.txt', 's3-file-bucket', '/uploads/stocks.txt')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Creates an object with the file content at '/uploads/stocks.txt' in the bucket.</p>
<p></p>
## Sink

### s3 *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">S3 sink publishes events as Amazon AWS S3 buckets.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="s3", credential.provider.class="<STRING>", aws.access.key="<STRING>", aws.secret.key="<STRING>", bucket.name="<STRING>", aws.region="<STRING>", versioning.enabled="<BOOL>", object.path="<STRING>", storage.class="<STRING>", content.type="<STRING>", bucket.acl="<STRING>", node.id="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">credential.provider.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS credential provider class to be used. If blank along with the username and the password, default credential provider will be used.</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.access.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS access key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.secret.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS secret key. This cannot be used along with the credential.provider.class</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Name of the S3 bucket</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">aws.region</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The region to be used to create the bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">versioning.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Flag to enable versioning support in the bucket</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">object.path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Path for each S3 object</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">storage.class</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">AWS storage class</p></td>
        <td style="vertical-align: top">standard</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">content.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Content type of the event</p></td>
        <td style="vertical-align: top">application/octet-stream</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bucket.acl</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Access control list for the bucket</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">node.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The node ID of the current publisher. This needs to be unique for each publisher instance as it may cause object overwrites while uploading the objects to same S3 bucket from different publishers.</p></td>
        <td style="vertical-align: top">EMPTY_STRING</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='s3', bucket.name='user-stream-bucket',object.path='bar/users', credential.provider='software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider', flush.size='3',
    @map(type='json', enclosing.element='$.user', 
        @payload("""{"name": "{{name}}", "age": {{age}}}"""))) 
define stream UserStream(name string, age int);  
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This creates a S3 bucket named 'user-stream-bucket'. Then this will collect 3 events together and create a JSON object and save that in S3.</p>
<p></p>
