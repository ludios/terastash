terastash
===

terastash is a half-baked filesystem for storing a lot of data in Google Drive.  It has been used in production at one site since June 2015 to store petabytes of data.

terastash stores all metadata (e.g. filenames and directory structure) locally and file contents remotely in one or more Google Drive accounts.  The file contents are chunked, encrypted with AES-GCM, and padded (in a poor attempt to conceal file sizes).

You should probably use [rclone](https://rclone.org/) instead, unless you really know what you're doing.


Things that appear to work for me
---

* Adding, retrieving, listing, and dropping files using a command line interface

* Arbitrarily nested subdirectories

* Storing very large files

* Storing tiny files inline in the Cassandra database

* Many retries to avoid failing when running over the daily upload limit

* Using more than one Google Drive account

* Random seek / reading from some arbitrary location in a file

* HTTP server for read-only access (working, but not rigorously debugged)

* Data integrity being guaranteed on retrieval through use of AES-GCM


Limitations
---
* No FUSE or 9P interface; adding/retrieving files requires `ts` commands

    * The database doesn't use inodes with 64-bit identifiers, making FUSE implementation more difficult than it should be

* Operations on many files often require `find . -type f` and `xargs`

* Files can't be renamed (but can be moved to other directories)

* Cassandra is used for no good reason; PostgreSQL would have worked better.

* Small files are not combined into one "pack" file for Google Drive.

* `ts` takes 300ms to start up because node needs to compile JavaScript every time

* `ts export-db` runs out of memory on large databases because of [a bug in the node Cassandra driver](https://github.com/datastax/nodejs-driver/pull/89#issuecomment-141602222).  The only reliable way to back up a large database is to back up the Cassandra data directories.


Notice
---
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

You have to be doubly crazy to store your data in Google Drive and then trust some JavaScript software to manage it.  You will probably lose your data.  Do not blame me, I warned you.
