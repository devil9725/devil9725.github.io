import os
import shutil
from sqlparse import parse
from sqlcomplete.parseutils.ctes import (
    token_start_pos, extract_ctes,
    extract_column_names as _extract_column_names)
from sqlcomplete.parseutils.tables import extract_tables

def extract_column_names(sql):
    p = parse(sql)[0]
    return _extract_column_names(p)



sql = '''

'''

ctes, remainder = extract_ctes(sql)

print (ctes)
    
#print (extract_column_names(sql))

counter = 1
name_target = "GRP_FUCK.B_O_O"
name_target_dir = name_target
if os.path.exists(name_target_dir):
    shutil.rmtree(name_target_dir)
if not os.path.exists(name_target_dir):
    os.makedirs(name_target_dir)
print (extract_tables(sql))

prev_table = ""
refList = []
for ts in extract_tables(sql):
    if ts.schema is not None:
        refList.append("--ref("+str(ts.schema)+"."+ str(ts.name)+")");
for cte in ctes:
    for ts in extract_tables(sql[cte.start+1:cte.stop-1]):
        if ts.schema is not None:
            refList.append("--ref("+str(ts.schema)+"."+ str(ts.name)+")");
refList = list(dict.fromkeys(refList))
prev_table = "\n".join(refList)

def gen_create(fname,columns,val):    
    return "{{ config(\ndag = ''\n)}}\n\n"+"CREATE TABLE "+fname+ " (\n"+'type ,\n'.join(columns)+"\n);\n"+"INSERT INTO "+fname+"\n"+val+";\n" + prev_table 


for cte in ctes:
    tmp_name = "grp_wrk."+name_target.replace(".","_")+"_"+str(counter)
    f = open(name_target_dir+"\\"+tmp_name+".sql", "x")
    f.write(gen_create(tmp_name,cte.columns,sql[cte.start+1:cte.stop-1]))
    f.close()
    counter = counter + 1
    prev_table = "--ref ("+tmp_name+")";
    print(cte.name)
    
tmp_name = "grp_wrk."+name_target.replace(".","_")+"_"+str(counter)
f = open(name_target_dir+"\\"+tmp_name+".sql", "x")
f.write(gen_create(tmp_name,extract_column_names(sql),remainder))
f.close()


