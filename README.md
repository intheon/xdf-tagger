# xdf-tagger
This is a simple command-line tool for adding/removing metadata tags from one 
or more XDF files. You can run it as in the (deliberately complex) example:

```
xdf-tagger.py --set subject.name="My Name" --set subject.id=subj001 --clear subject.handedness --show subject.age *.xdf
```

You can also run it as:
```
python xdf-tagger.py  <command-line arguments>
```

This script writes tags into a stream named `Metadata`, with type `Metadata`,
which will be created if not already present. This way, tags managed by this 
script are sandboxed from other meta-data.

The script can be used to associate, for instance, human-subject or experiment
metadata as defined in the [XDF specification](https://github.com/sccn/xdf/wiki/Meta-Data).
