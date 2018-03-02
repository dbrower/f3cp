F3CP
====

F3CP is a simple object export/ingest utility.
It will copy a list of object from fedora 3 into a JSON file.
It will copy a JSON file (in a suitable format) into a fedora 3.

# Usage

To copy a few objects into a JSON file:

    f3cp https://user:pass@host/fedora  temp:123 temp:124 temp:125 > out.json

To copy the items from a JSON file into fedora:

    f3cp out.json https://user:pass@host/fedora

To copy all the pids in a given file from one fedora to another:

    cat all.pids | f3cp https://user:pass@host1/fedora | f3cp https://user:pass@host2/fedora

The previous will stream the objects, so only one object is held in memory at a time.

