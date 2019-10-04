# xdf-tagger
Simple command-line tool for adding/removing metadata tags from XDF files
        
Tags will be written into a stream named Metadata, of type Metadata, 
and the stream will be created if not already present.

You can use arguments like --set and --clear multiple times to set/clear
multiple tags in a single run of the tool.

Example:
```
xdf-tagger --set subject.name="My Name" --set subject.id=subj001 --clear subject.handedness --show subject.age *.xdf
```
