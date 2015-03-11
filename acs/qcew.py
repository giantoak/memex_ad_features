import glob
import pandas
outputs = []
for i in glob.glob('*singlefile*csv'):
    with open(i,'r') as f:
        print('reading file: %s' % i)
        new = pandas.read_csv(i)
        new = new[new.agglvl_code==44]
        outputs.append(new)
#a=pandas.read_csv("2014.q1-q2.singlefile.csv")
#a=a[a.agglvl_code == 44]   # Select only major industry
data = pandas.concat(outputs, axis=0)
