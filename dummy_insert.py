from sqlalchemy.orm import sessionmaker
from db import engine, User1, User2

User1.__table__.create(bind=engine, checkfirst=True)
User2.__table__.create(bind=engine, checkfirst=True)

Session = sessionmaker(bind = engine)
session = Session()
user = User1(first_name='Sajeesh', last_name='Saby', address='437/ E', age=10)

session.add(user)
session.commit()