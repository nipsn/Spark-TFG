#!/bin/sh
filename="${1%.*}"

perl -0777 -pe 's/Map\((.*?)\)/{$1}/sg' $1 | \
perl -0777 -pe 's/({.*?}).withDefaultValue\((.*?)\)/defaultdict(lambda: $2, $1)/sg' | \
perl -0777 -pe 's/((\bList\b)|(\bSeq\b)|(\bArray\b))\((([ |\n]*"[^\n"]*"([ |\n]*,[ |\n]*"[^\n"]*"[ |\n]*)*)|([ |\n]*[^\n"\)]*([ |\n]*,[ |\n]*"[^\n"]*"[ |\n]*)*))\)/[$5]/sg' | \
sed -E -f scriptfile.sed > $filename.py;
printf "\n\n" >> $filename.py



