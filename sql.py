import csv
import sys
import re
import os
import numbers
import sqlparse as sp
import itertools
metadata={}
def readMetadata():
    f=open('./files/metadata.txt','r')
    check=0
    for line in f:
        if line.strip()=="<begin_table>":
            check=1
            continue
        if check==1:
            tablename=line.strip()
            metadata[tablename]=[]
            check=0
            continue
        if not line.strip()== "<end_table>":
            metadata[tablename].append(line.strip())
    for i in metadata:
        metadata[i]=list(filter(None,metadata[i]))
def outerjoin(par1):
    values=[]
    for j in range(len(par1)):
        path = 'files/'+ par1[j] + '.csv'
        f = open(path,'r')
        reader=csv.reader(f)
        val=[]
        for row in reader:
                val.append(row)
        values.append(val)
    ans=list(itertools.product(*values))
    return ans

def Select_all(parsed,idlist,ind,prin):
    par1 = re.sub("[,]",' ',idlist[ind]).split()
    ans=outerjoin(par1)
    if(prin==1):
        for j in range(len(par1)):
            path = 'files/'+ par1[j] + '.csv'
            f = open(path,'r')
            reader=csv.reader(f)
            for i in metadata[par1[j]]:
                print(par1[j]+"."+i+'\t',end=' ')
        print('\n')
    if(prin==1):
        for i in range(len(ans)):
            for j in range(len(ans[i])):
                for k in range(len(ans[i][j])):
                    print(str(ans[i][j][k])+'\t\t',end=' ')
            print('\n')
    return ans
def get_col(file,att,c=0):
    m=open('files/metadata.txt')
    col=0
    found=0
    flag=0
    for line in m:
        if(line.strip() == file):
            flag=1
        if(flag==1):
            if(line.strip()=="<end_table>"):
                print("Attribute doesn't exsist")
                return
            if(line.strip()!=att):
                col+=1
            if(line.strip()==att):
                found=1
                break
    m.close()
    if(c==1):
        return col
    return col
def aggregate(par,parsed,idlist,ans):
    # reader=csv.reader(f)
    att=par[1]
    # print(att)
    col=get_col(idlist[3],att)
    val=[]
    for i in range(len(ans)):
        for j in range(len(ans[i])):
            if isinstance(ans[i][j][col-1],str):
                ans[i][j][col-1]= int(ans[i][j][col-1])
            val.append(ans[i][j][col-1])
    print(idlist[1])
    if(par[0]=="max"):
        print(max(val))
    elif(par[0]=="min"):
        print(min(val))
    elif(par[0]=="sum"):
        print(sum(val))
    elif(par[0]=="average"):
        print(sum(val)/len(val))
def get_col_indices(par,tables):
    cols=par
    colnos=[]
    for att in cols:
        if(len(att.split('.'))==1):
            sweep=0
            for tab in tables:
                m=open('files/metadata.txt')
                col=0
                found=0
                flag=0
                for line in m:
                    if(line.strip() == tab):
                        flag=1
                    if(flag==1):
                        if(line.strip()!=att):
                            col+=1
                        if(line.strip()=="<end_table>"):
                            break
                        if(line.strip()==att):
                            found=1
                            colnos.append([sweep,col-1])
                            break
                if(found==1):
                    break
                sweep+=1
        else:
            temp=att.split('.')[1]
            tab=att.split('.')[0]
            m=open('files/metadata.txt')
            col=0
            found=0
            flag=0
            for line in m:
                if(line.strip() == tab):
                    flag=1
                if(flag==1):
                    if(line.strip()!=temp):
                        col+=1
                    if(line.strip()==temp):
                        found=1
                        colnos.append([tables.index(tab),col-1])
                        break
            m.close()
    return colnos
    
def project(par,idlist,tind,prin,ans):
    cols=par
    tables=idlist[tind].split(',')
    colnos=get_col_indices(par,tables)
    # print(colnos)
    for i in cols:
            print(i+'\t\t',end=' ')
    print('\n')
    values=[]
    for i in range(len(ans)):
        val=[]
        for j,k in colnos:
            val.append(ans[i][j][k])
            if(prin==1):
                print(str(ans[i][j][k])+'\t\t',end=' ')
        if(prin==1):
            print('\n')
        values.append(val)
    return values
def check(val1,val2,op):
    val1=int(val1)
    val2=int(val2)
    if(op=="="):
        if(val1==val2):
            return True
    if(op=="<"):
        if(val1<val2):
            # print("kmt")
            return True
    if(op==">"):
        if(val1 > val2):
            return True
    if(op=="<="):
        if(val1<=val2):
            return True
    if(op==">="):
        if(val1>=val2):
            return True
    return False
def update_condition(conditions,ans,idlist):
    operators=["=",">","<","<=",">="]
    updated_ans=[]
    colnos=[]
    op="="
    second=0
    if(len(conditions)==1):
        # print(conditions)
        for operator in operators:
            cols=conditions[0].split(operator)
            if(operator=="="):
                cols[0]=cols[0].split("<")
                if(len(cols[0])>1):
                    operator="<="
                    cols[0]=cols[0][0]
                else:
                    # print(cols[0])
                    cols[0]=("".join(cols[0])).split(">")
                    if(len(cols[0])>1):
                        operator=">="
                        cols[0]=cols[0][0]
                    else:
                        cols[0]="".join(cols[0])
                    
            # print(cols)
            if(len(cols)==2):
                op=operator
                colnos=get_col_indices(cols,idlist[3].split(","))
                if(cols[1].isdigit()):
                    colnos.append(cols[1])
                    second=1
                # print(colnos)
                break
        # print(colnos)
        for i in range(len(ans)):
            if(second==0):
                # temp=check(ans[i][colnos[0][0]][colnos[0][1]], ans[i][colnos[1][0]][colnos[1][1]],op)
                # print("kmt "+str(ans[i][colnos[0][0]][colnos[0][1]])+" "+str(ans[i][colnos[1][0]][colnos[1][1]])+" "+op+" "+str(temp))
                if(check(ans[i][colnos[0][0]][colnos[0][1]], ans[i][colnos[1][0]][colnos[1][1]],op)):
                    updated_ans.append(ans[i])
            if(second==1):
                if(check(ans[i][colnos[0][0]][colnos[0][1]], colnos[1],op)):
                    # print("krishna")
                    updated_ans.append(ans[i])
        # if(second==0 and op=="="):
        #     del a[:][colnums[0][0]][colnums[0][1]]
    if(second==0 and idlist[1].split(",")[0]=="*"):
        # print("krishna")
        for i in range(len(updated_ans)):
            del updated_ans[i][colnos[0][0]][colnos[0][1]]
    return updated_ans
def where(idlist,q,prin):
    clause=idlist[4].split("where")
    # clause=re.sub("[ ]",'',clause)
    clause[1]=re.sub("[ ]",'',clause[1])
    clause=clause[1]
    # print(clause)
    colnos=[]
    a="AND" in q
    o="OR" in q
    if a:
        conditions=clause.split("AND")
    elif o:
        conditions=clause.split("OR")
    else:
        conditions=clause.split()
    operators=["=",">","<","<=",">="]
    ans=outerjoin(idlist[3].split(","))
    if(idlist[1].split(",")[0]=="*"):
        tables=idlist[3].split(',')
        # print(tables)
        for tab in tables:
            for z in metadata[tab]:
                print(z,end="\t\t")
        print("\n")
    if(len(conditions)==1):
        updated_ans=update_condition(conditions,ans,idlist)
        if(prin==0):
            return updated_ans
        par=idlist[1].split(",")
        if(par[0]=="*"):
            for i in range(len(updated_ans)):
                for j in range(len(updated_ans[i])):
                    for k in range(len(updated_ans[i][j])):
                        print(str(updated_ans[i][j][k])+'\t\t',end=' ')
                print('\n')
        else:
            project(par,idlist,3,1,updated_ans)
    else:
        if a:
            updated_ans=update_condition([conditions[0]],ans,idlist)
            updated_ans2=update_condition([conditions[1]],updated_ans,idlist)
            updated_ans=updated_ans2
            if(prin==0):
                return updated_ans
            par=idlist[1].split(",")
            if(par[0]=="*"):
                for i in range(len(updated_ans)):
                    for j in range(len(updated_ans[i])):
                        for k in range(len(updated_ans[i][j])):
                            print(str(updated_ans[i][j][k])+'\t\t',end=' ')
                    print('\n')
            else:
                project(par,idlist,3,1,updated_ans)
        if o:
            updated_ans=update_condition([conditions[0]],ans,idlist)
            updated_ans2=update_condition([conditions[1]],ans,idlist)
            for i in range(len(updated_ans2)):
                if(updated_ans2[i] not in updated_ans):
                    updated_ans.append(updated_ans2[i])
            if(prin==0):
                return updated_ans
            par=idlist[1].split(",")
            if(par[0]=="*"):
                for i in range(len(updated_ans)):
                    for j in range(len(updated_ans[i])):
                        for k in range(len(updated_ans[i][j])):
                            print(str(updated_ans[i][j][k])+'\t\t',end=' ')
                    print('\n')
            else:
                project(par,idlist,3,1,updated_ans)

def Select(parsed,idlist,q):

    par = re.sub("[\(\),]",' ',idlist[1]).split()
    if(par[0]=='*' and len(idlist)==4):
        Select_all(parsed,idlist,3,1)
    elif((par[0]=='max' or par[0]=='sum' or par[0]=='min' or par[0]=='avg') and "where" not in q):
        aggregate(par,parsed,idlist,outerjoin(idlist[3].split(",")))
    elif((par[0]=='max' or par[0]=='sum' or par[0]=='min' or par[0]=='avg' )and "where"  in q):
        ans=where(idlist,q,0)
        aggregate(par,parsed,idlist,ans)
    elif (par[0]!="*" and ("where" not in q) and len(idlist)==4 ):
        tind=idlist[3]
        project(par,idlist,3,1,outerjoin(idlist[3].split(",")))
    elif(par[0]=="distinct"):
        par1 = re.sub("[\(\),]",' ',idlist[2]).split()
        values=project(par1,idlist,4,0,outerjoin(idlist[4].split(",")))
        values = list(set(tuple(x) for x in values))
        for i in range(len(values)):
            for j in range(len(values[i])):
                print(values[i][j],end='\t\t')
            print("\n")
    elif(("where" in q)):
        where(idlist,q,1)

def check_tables(tables):
    present=True
    tab=""
    for tab in tables:
        if tab in metadata.keys():
            continue
        else:
            present=False
    if present==False:
        print(" No Table with name "+tab+" in database")
        return True
    return False
        
def check_attributes(par,tables):
    # par=idlist[2].split(",")
    for col in par:
        present=True
        c=col.split(".")
        if(len(c)==2):
            # print(c)
            if c[1] not in metadata[c[0]]:
                present=False
                print("Attribute "+c[1]+" not in table "+c[0])
        if(len(c)==1):
            there=False
            for k in tables:
                if( c[0] in metadata[k]):
                    there=True
                    break
            if(there==False):
                present=False
                print("attribute "+c[0]+" not present in any of the tables")
        if(present==False):
            return True
    return False

def error_check(q,idlist):
    if(len(idlist)<4):
        if(idlist[0]!="select"):
            print("query not of type select,please check")
        if(idlist[2]!="from" and idlist[3]!="from"):
            print("\"from\" word missing in query")
        print("invalid query")
        return True
    if(("where" in q) and len(idlist)<5):
        print("invalid query containing \"where\" keyword")
        return True
    if(("where" not in q) and len(idlist)>5):
        print("invalid query")
        return True
    if("distinct" in q ):
        if(idlist[1]!="distinct"):
            print("invalid query word Distinct not in correct position")
            return True
        else:
            if(check_tables(idlist[4].split(","))==True):
                return True            
            if(idlist[2]=="*"):
                pass
            else:
                if(check_attributes(idlist[2].split(","),idlist[4].split(","))==True):
                    return True


    else:
        func=idlist[1].split("(")
        if(len(func)>1):
            if(func[0] not in ["max","min","sum","average"]):
                print("Function "+func+" not present")
                return True
            col=func[1].split(")")
            if(check_attributes(col[0],idlist[3].split(","))==True):
                return True
        else:
            if(check_tables(idlist[3].split(","))==True):
                    return True            
            if(idlist[1]=="*"):
                pass
            else:
                if(check_attributes(idlist[1].split(","),idlist[3].split(","))==True):
                    return True
    if("where" in q):
        clause=idlist[4].split("where")
        clause[1]=re.sub("[ ]",'',clause[1])
        clause=clause[1]
        conditions=clause.split("AND")
        conditions=clause.split("OR")
        if(len(conditions)>2):
            print("maximum possible and and or is one but count is more in given query")
            return True
    return False     

        
    
 
def process(q):
    parsed=sp.parse(q)[0].tokens
    qtype=sp.sql.Statement(parsed).get_type()
    idlist=[]
    l=sp.sql.IdentifierList(parsed).get_identifiers()
    for i in  l:
        idlist.append(str(i))
    if error_check(q,idlist)==True:
        return
    if qtype=='SELECT':
        Select(parsed,idlist,q)
def main():
    readMetadata()
    # print(metadata)
    # while(1):
        # print("myquery>")
        # query=input()
    query=str(sys.argv[1])
    # print(query)
    if query=="exit":
        return
    else:
        process(query)
if __name__ == "__main__":
	main()