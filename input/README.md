# input/
This directory should contain the following for desired languages:
- Wiki bz2 dumps: both the .xml.bz2 the index.txt.bz2 files from https://dumps.wikimedia.org/backup-index.html
- dictionary files, one of the following per language:
  - txt, one word per line
  - json, of the format ["word1","word2",...]
  - xml, as per https://www.edrdg.org/wiki/index.php/JMdict-EDICT_Dictionary_Project

Dictionary sources are listed in the accompanying WordIcebergs.java file in this repository.
