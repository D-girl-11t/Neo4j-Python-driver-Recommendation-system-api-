#basic content

#libraries
from neo4j import GraphDatabase
import pandas as pd

#data
udata= pd.read_csv("C:\\Users\\DHANA\\Downloads\\user_data.csv")
edata=pd.read_csv("C:\\Users\\DHANA\\Downloads\\event_data.csv")
cdata=pd.read_csv("C:\\Users\\DHANA\\Downloads\\foodtype - foodtype.csv")

#for i in udata['preference']:
    

#neo4j_driver
class RECC(object):
    def __init__(self,url,user,pas):
        self._driver= GraphDatabase.driver(url,auth=(user,pas),encrypted=False)
        
    def close(self):
        self._driver.close()
    
    def Query(self,query,parameters):
        with self._driver.session() as session:
            q=session.write_transaction(self._readq_,query,parameters)
            return q
    @staticmethod
    def _readq_(tx,query,parameters):
        result= tx.run(query,parameters)
        res= pd.DataFrame(result.records(), columns = result.keys())
        
        
        return res
k= RECC("bolt://54.236.223.96:34373","neo4j","diameter-restaurant-amplitudes")

#loading_data

USERS="""merge(u:Users{title:$userID, uname:$uname, uage:$uage, ugender:$ugender,
ucontact:$phone_number, umailid:$umailid, ulocation:$ulocation,ucountry:$ucountry,
upreference:$upreference, uprofession:$uprofession})

RETURN u"""
EVENTS="""merge (u:Events{title: $eventid , ename: $ename , elocation:$elocation,
whocanjoin:$who, ecategory:$ecategory, efoodtype:$efoodtype ,
ecreatorID:$ecreatorID})
RETURN u"""

LOCATIONS="""MERGE (l:Location{lname:$ulocation})
return l"""
CUISINES="""merge (c:FoodType{cname:$food})
RETURN c"""
import ast

#making nodes
for i,r in udata.iterrows():
    pref= ast.literal_eval(r.preference)
    u= k.Query(USERS,{'userID':r.id,'uname':r.firstName,'uage':r.age,
                    'ugender':r.gender,'phone_number':r.phoneNumber,'umailid':r.email,'ucountry':r.countryCode,
                    'upreference':pref,'ulocation':r.location,'uprofession':r.profession})
    l=k.Query(LOCATIONS,{'ulocation':r.location})
   
        
        
for i,r in edata.iterrows():
    e=k.Query(EVENTS,{'eventid':r.id,'ename':r.ename,'elocation':r.location,'ecategory':r.category,
                      'efoodtype':r.foodType,'ecreatorID':r.creatorID, 'who':r.anyoneCanJoin})
       
for i, r in cdata.iterrows():
    c=k.Query(CUISINES,{'food':r.cuisine__cuisine_name})
    
    


qry = """match(n:Users)
return n"""
ss=k.Query(qry,{})
print(ss)

#making relations
URELATION="""match(a:Users),(b:Location)
where (a.ulocation)=(b.lname)
merge(a)-[d:user_location]->(b)
return d"""
k.Query(URELATION,{})


#############################################
posted="""
match (a:Users),(b:Events)
where (a.title)=(b.ecreatorID)
merge (a)-[:POSTEDevent]->(b)
return b"""
k.Query(posted,{})



ERELATION="""match (a:Events),(b:Location)
where (a.elocation)= (b.lname)
merge (a)-[r:event_location]->(b)
return r"""
k.Query(ERELATION,{})

UFOOD="""match (a:Users{title:$id})
unwind (a.upreference) as food
match (food), (b:FoodType)
where food= b.cname
merge (a)-[:Likes_this]->(b)
return b"""

for i,r in udata.iterrows():
    k.Query(UFOOD,{'id':r.id})
    
            


EFOOD="""MATCH (a:Events),(b:FoodType)
where (a.efoodtype)=(b.cname)
merge (a)-[t:plans]->(b)
return t"""
k.Query(EFOOD,{})



#cor="""match(a:Users{ulocation:$ul}),(b:Events{elocation:$ul})
#return (a.title),collect(b.ename)"""
#conrec=k.Query(cor,{'ul':uid})
#api
from flask import Flask, render_template, request,redirect, url_for, jsonify
app=Flask(__name__)
@app.route('/',methods=['GET','POST'])
def userlogin():
    if request.method=='POST':
        k.Query(USERS,{'userID':request.form['uid'],'uname':request.form['uname'],'uage':request.form['age'],
                    'ugender':request.form['ugender'],'phone_number':request.form['number'],'umailid':request.form['mail'],'ucountry':request.form['countryCode'],
                    'ulocation':request.form['location'],'upreference':request.form['preferance'],'uprofession':request.form['profession']})
        #ans= pd.concat([userdata,d],ignore_index=True)
        ul= request.form['location']
        #make relations ....
        k.Query(URELATION,{})
        for i,r in udata.iterrows():
            k.Query(UFOOD,{'id':r.id})
        
        return redirect(url_for("option",u=ul))
    
    else:
        return render_template('zx.html')


@app.route('/<u>', methods=['GET','POST'])
def option(u):
    if request.method=='POST':
        return redirect(url_for("event"))
    else:
        return render_template('cho.html',uk=u) 

@app.route('/results/<uk>',methods=['GET','POST'])
def choose(uk):
    if request.method=='POST':
        cor="""match(a:Users),(b:Events)
where (a.ulocation)=(b.elocation)=$ul or (a.title)=(b.ecreatorID)
return (a.title),collect(b.ename)"""
        conrec=k.Query(cor,{'ul':uk})
        x=conrec.to_json(orient='records')
        return x
    else:
        return render_template('cho.html')

@app.route('/event',methods=['GET','POST'])
def event():
    if request.method == 'POST':
        k.Query(EVENTS,{'eventid':request.form['eid'],'ename':request.form['ename'],'elocation':request.form['location'],'ecategory':request.form['ecategory'],'who':request.form['anyone'],
                      'efoodtype':request.form['foodType'],'ecreatorID':uid})#ecreator
       
        k.Query(posted,{})       
        return '<h1>ENJOY FUG*it</h1>'
    else:
        return render_template('eve.html')


app.run()


#---------------------------------------------------------------------------


























