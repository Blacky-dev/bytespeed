from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, func, Index,ForeignKey,ForeignKeyConstraint,asc,text,case,and_,exists
from sqlalchemy.orm import sessionmaker,joinedload
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session,relationship
from fastapi import FastAPI, HTTPException, Depends
import uvicorn,sys
from pydantic import BaseModel
from sqlalchemy.orm import selectinload

SQLALCHEMY_DATABASE_URL = "postgresql://postgres:admin@localhost/test"
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Create a base class for declarative class definitions
Base = declarative_base()
class ContactOrdersPayload(BaseModel):
    email: str=None
    phoneNumber: str=None

class ContactOrders(Base):
    __tablename__ = "ContactOrders"

    id = Column(Integer, primary_key=True, index=True,autoincrement=True)
    phoneNumber = Column(String(20))
    email = Column(String(255))
    linkedId = Column(Integer, ForeignKey("ContactOrders.id",ondelete='CASCADE'),default=None)
    linkPrecedence = Column(String(10),default='primary')
    createdAt = Column(TIMESTAMP, default=func.now())
    updatedAt = Column(TIMESTAMP, default=func.now())
    # ForeignKeyConstraint(['linkedId'], ['ContactOrders.id'], ondelete='CASCADE')
    # children = relationship('ContactOrders', remote_side=[id],backref="parent",order_by='ContactOrders.createdAt')
    parent = relationship('ContactOrders', remote_side=[id], back_populates="children", uselist=False)
    children = relationship('ContactOrders', back_populates="parent", order_by='ContactOrders.createdAt')

Index('unique_phone_email', ContactOrders.phoneNumber, ContactOrders.email, unique=True)

# Create tables in the database
Base.metadata.create_all(engine)

# Create a sessionmaker bound to the engine
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create the FastAPI app

class ContactResponse(BaseModel):
    primaryContactId: int=None
    emails: list[str]=[]
    phoneNumbers: list[str]=[]
    secondaryContactIds: list[int]=[]


app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def existing_saved_record(payload,db):
    """this function searches for rec where same row has phone and email and escapes"""
    try:
        contact_orders = db.query(ContactOrders).filter(and_(ContactOrders.phoneNumber == payload.phoneNumber, ContactOrders.email == payload.email)).one()
        # print(contact_orders.__dict__,'======')
        if contact_orders.linkedId!=None:
            print('Trueeeeeeeeeeeeeeeeeeee')
            new_contact_order = ContactOrders(phoneNumber=payload.phoneNumber, email=payload.email,linkPrecedence='secondary',linkedId=contact_orders.linkedId)
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.id == contact_orders.linkedId).one()
            child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
            response = ContactResponse(
                primaryContactId=contact_orders.__dict__['id'],
                emails=[contact_orders.__dict__['email']]+[contact['email'] for contact in child_rows],
                phoneNumbers=[contact_orders.__dict__['phoneNumber']]+[contact['phoneNumber'] for contact in child_rows],
                secondaryContactIds=[contact['id'] for contact in child_rows if contact['linkPrecedence']!='primary']
            )
            return response
        else:
            print('////////////////////|||||')
            # print(existing_record.__dict__)
            new_contact_order = ContactOrders(phoneNumber=payload.phoneNumber, email=payload.email,linkPrecedence='secondary',linkedId=contact_orders.id)
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.id == contact_orders.id).one()
            child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
            response = ContactResponse(
                primaryContactId=contact_orders.__dict__['id'],
                emails=[contact_orders.__dict__['email']]+[contact['email'] for contact in child_rows],
                phoneNumbers=[contact_orders.__dict__['phoneNumber']]+[contact['phoneNumber'] for contact in child_rows],
                secondaryContactIds=[contact['id'] for contact in child_rows if contact['linkPrecedence']!='primary']
            )
            return response
    except:
        return None

def linking_rec(existing_record,payload,db):
    exists_phn_and_email = exists().where(ContactOrders.email == payload.email) & exists().where(ContactOrders.phoneNumber == payload.phoneNumber)
    contact_orders= db.query(ContactOrders).filter(exists_phn_and_email).first()
    # print(phn_email_found_on_one_more_row,'==')
    if not contact_orders:
        existing_record = db.query(ContactOrders).filter(((ContactOrders.phoneNumber == payload.phoneNumber) | (ContactOrders.email == payload.email))).first()
        if existing_record.linkedId!=None:
            print('Trueeeeeeeeeeeeeeeeeeee')
            new_contact_order = ContactOrders(phoneNumber=payload.phoneNumber, email=payload.email,linkPrecedence='secondary',linkedId=existing_record.linkedId)
            db.add(new_contact_order)
            db.commit()
            db.refresh(new_contact_order)
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.id == existing_record.linkedId).one()
            child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
            response = ContactResponse(
                primaryContactId=contact_orders.__dict__['id'],
                emails=[contact_orders.__dict__['email']]+[contact['email'] for contact in child_rows],
                phoneNumbers=[contact_orders.__dict__['phoneNumber']]+[contact['phoneNumber'] for contact in child_rows],
                secondaryContactIds=[contact['id'] for contact in child_rows if contact['linkPrecedence']!='primary']
            )
            return response
        else:
            print('////////////////////|||||')
            # print(existing_record.__dict__)
            new_contact_order = ContactOrders(phoneNumber=payload.phoneNumber, email=payload.email,linkPrecedence='secondary',linkedId=existing_record.id)
            db.add(new_contact_order)
            db.commit()
            db.refresh(new_contact_order)
            # print(db.query(ContactOrders).get(new_contact_order.linkedId),'======')
            # print(ContactOrders.__dict__)
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.id == existing_record.id).one()
            child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
            response = ContactResponse(
                primaryContactId=contact_orders.__dict__['id'],
                emails=[contact_orders.__dict__['email']]+[contact['email'] for contact in child_rows],
                phoneNumbers=[contact_orders.__dict__['phoneNumber']]+[contact['phoneNumber'] for contact in child_rows],
                secondaryContactIds=[contact['id'] for contact in child_rows if contact['linkPrecedence']!='primary']
            )
            return response
    else:
        child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
        response = ContactResponse(
            primaryContactId=contact_orders.__dict__['id'],
            emails=[contact_orders.__dict__['email']]+[contact['email'] for contact in child_rows],
            phoneNumbers=[contact_orders.__dict__['phoneNumber']]+[contact['phoneNumber'] for contact in child_rows],
            secondaryContactIds=[contact['id'] for contact in child_rows if contact['linkPrecedence']!='primary']
        )
        return response

def serch_by_mail_or_phn(existing_record,db,payload):
    if payload.email is None:
        if existing_record.linkedId:
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.parent)).filter(ContactOrders.phoneNumber==payload.phoneNumber).first()
            parent_id=contact_orders.parent.id
            emails=[contact_orders.parent.email]+[i.__dict__['email'] for i in contact_orders.parent.children]
            phone=[contact_orders.parent.phoneNumber]+[i.__dict__['phoneNumber'] for i in contact_orders.parent.children]
            secondaryid=[i.__dict__['id'] for i in contact_orders.parent.children]
        else:
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.phoneNumber==payload.phoneNumber).first()
            # contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.email==payload.email).first()
            parent_id=contact_orders.id
            # parent_id=contact_orders.parent.id
            emails=[contact_orders.email]+[i.__dict__['email'] for i in contact_orders.children]
            phone=[contact_orders.phoneNumber]+[i.__dict__['phoneNumber'] for i in contact_orders.children]
            secondaryid=[i.__dict__['id'] for i in contact_orders.children]

    else:
        print('bxhdhbchdbhdbhb')
        if existing_record.linkedId:
        # query = db.filter(ContactOrders.email == payload.email)
            print('==== found linkedid ==========')
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.parent)).filter(ContactOrders.email==payload.email).first()
            parent_id=contact_orders.parent.id
            emails=[contact_orders.parent.email]+[i.__dict__['email'] for i in contact_orders.parent.children]
            phone=[contact_orders.parent.phoneNumber]+[i.__dict__['phoneNumber'] for i in contact_orders.parent.children]
            secondaryid=[i.__dict__['id'] for i in contact_orders.parent.children]
            # child_rows=[i.__dict__ for i in contact_orders.parent.children]
            # print(parent_id)
            # print(contact_orders.parent.children)
            # print([i.__dict__ for i in contact_orders.__dict__['parent']])
        else:
            print('==== primary row ==========')
            contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.email==payload.email).first()
            parent_id=contact_orders.id
            # parent_id=contact_orders.parent.id
            emails=[contact_orders.email]+[i.__dict__['email'] for i in contact_orders.children]
            phone=[contact_orders.phoneNumber]+[i.__dict__['phoneNumber'] for i in contact_orders.children]
            secondaryid=[i.__dict__['id'] for i in contact_orders.children]
    # contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.parent)).filter(ContactOrders.).all()
    # print(contact_orders.__dict__)
    # child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
    response = ContactResponse(
        primaryContactId=parent_id,
        emails=emails,
        phoneNumbers=phone,
        secondaryContactIds=secondaryid
    )
    return response

def unique_record(payload,db):
    new_contact_order = ContactOrders(phoneNumber=payload.phoneNumber, email=payload.email,linkPrecedence='primary')
    # print(new_contact_order.childern)
    db.add(new_contact_order)
    db.commit()
    db.refresh(new_contact_order)
    contact_orders = [db.query(ContactOrders).options(joinedload(ContactOrders.parent)).filter(ContactOrders.phoneNumber == new_contact_order.phoneNumber).one()]
    print(contact_orders)
    for order in contact_orders:
        print(f"Contact Order ID: {order.id}, Parent ID: {order.parent.id if order.parent else None}")
    print(new_contact_order)
    response = ContactResponse(
            primaryContactId=new_contact_order.linkedId,
            emails=[contact.email for contact in contact_orders],
            phoneNumbers=[contact.phoneNumber for contact in contact_orders],
            secondaryContactIds=[contact.id for contact in contact_orders if contact.id != new_contact_order.linkedId and new_contact_order.linkPrecedence!='primary']
        )
    return response

@app.post("/contact_orders/")
async def create_contact_order(payload:ContactOrdersPayload, db: Session = Depends(get_db)):
    try:
        
        existing_record = db.query(ContactOrders).filter(((ContactOrders.phoneNumber == payload.phoneNumber) | (ContactOrders.email == payload.email)))
       
        if existing_record and payload.phoneNumber!=None and payload.email!=None:
            response=existing_saved_record(payload,db)
            if response==None:
                print('--------- insert new record ------------')
                response=linking_rec(existing_record,payload,db)
            return response
        
        if existing_record and payload.phoneNumber==None or payload.email==None:
            print('---------- record by mail or phn number --------------')
            response=serch_by_mail_or_phn(existing_record,db,payload)
            return response
        
        if not existing_record:
            print('--------------- new unique record ----------')
            response=unique_record(payload,db)
            return response
        
    except IntegrityError as e:
        print(e,'===============')
        db.rollback()
        # query=db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.phoneNumber == existing_record.phoneNumber).all()
        contact_orders = db.query(ContactOrders).options(joinedload(ContactOrders.children)).filter(ContactOrders.phoneNumber == existing_record.phoneNumber).one()
        # for order in contact_orders:
        #     print(order)
        #     print(f"Contact Order ID: {order.id}, Parent ID: {order.children.id if order.children else None}")
        child_rows=[i.__dict__ for i in contact_orders.__dict__['children']]
        response = ContactResponse(
            primaryContactId=contact_orders.__dict__['id'],
            emails=[contact_orders.__dict__['email']]+[contact['email'] for contact in child_rows],
            phoneNumbers=[contact_orders.__dict__['phoneNumber']]+[contact['phoneNumber'] for contact in child_rows],
            secondaryContactIds=[contact['id'] for contact in child_rows if contact['linkPrecedence']!='primary']
        )
        return response
        # raise HTTPException(status_code=400, detail="Contact with the same email or phone number already exists.")

if __name__=='__main__':
    uvicorn.run(app,host="0.0.0.0",port=8001)