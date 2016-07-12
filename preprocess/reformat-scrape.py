# coding: utf-8
import re
# The files returned from the UCR export have extraneous 
# headers and footers. I am stripping them here. This is
# obviously very sensitive to the way the header is structured,
# so not intended for use beyond this hackathon...
# (9/19/14)

csv_full = ''

for i in range(2, 22):
    with open('crime_scrape2013_{}.csv'.format(i)) as f:
        r = f.read()[102:]
        r1 = re.sub(',\\r', '', r)
        r2 = re.sub('\\r', '', r1)
        r3 = re.sub('\\n\\n.*', '', r2)
        csv = r3[:-31]
        csv_clean = re.sub(', *', ',', csv)

        split = csv_clean.split('\n')
        header = split[0]

        csv_full += '\n'.join(split[1:]) + '\n'

full = header + '\n' + csv_full
with open('crime_scrape2013.csv', 'w') as f:
    f.write(full)
