from datetime import datetime
from sqlalchemy import create_engine, ForeignKey, String, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from sqlalchemy.sql.expression import func
import os


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "account"
    id: Mapped[int] = mapped_column(primary_key=True)
    account_name: Mapped[str] = mapped_column(String, unique=True)
    password: Mapped[str] = mapped_column(String)

    activity = relationship('Activity', uselist=False, back_populates='account')
    chats = relationship('Chats', back_populates='account')

    # def __repr__(self) -> str:
    #     return f"Account(id={self.id!r}, account_name={self.account_name!r}, password={self.password!r})"


class Activity(Base):
    __tablename__ = 'activity'
    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("account.id"), unique=True)
    last_login: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    api_limit_hit: Mapped[datetime] = mapped_column(DateTime, default=None, nullable=True)
    api_limit_reset: Mapped[datetime] = mapped_column(DateTime, default=None, nullable=True)

    account = relationship("Account", back_populates="activity")

    # def __repr__(self):
    #     return f"<Activity(id={self.id!r}, account_id={self.account_id!r}, last_login='{self.last_login!r}," \
    #            f" api_limit_hit='{self.api_limit_hit!r}, api_limit_reset='{self.api_limit_reset!r},')>"
    #


class Chats(Base):
    __tablename__ = 'chats'
    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("account.id"))
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now())
    model: Mapped[str] = mapped_column(String)
    message: Mapped[str] = mapped_column(String)
    completion: Mapped[str] = mapped_column(String)

    account = relationship('Account', back_populates='chats')

    # def __repr__(self):
    #     return f"<Chats(id={self.id!r}, account_id={self.account_id!r}, date='{self.date!r}," \
    #            f" modle='{self.model!r}, message='{self.megssage!r},completion='{self.completion!r},')>"


def initialize_engine(filename, echo=False):
    return create_engine(f"sqlite:///data/{filename}", echo=echo)


def initialize_tables(engine):
    Base.metadata.create_all(engine)


def ask_user():
    accounts = {}
    while True:
        account_name = str(input("Enter the account name (or 'q' to quit): "))
        if account_name.lower() == 'q':
            exit = input("Are you sure that you want to quit? Yes (y) or No (n)?")
            if exit.lower() == 'y':
                break
            else:
                continue

        password = input("Enter the password: ")
        accounts[account_name] = str(password)

    print("User accounts:")
    for account in accounts.items():
        print("Account Name:", account[0])
        print("Password:", account[1])
        print("------------------")

    return accounts


def initialize_db(replace=False):
    my_file = "accounts.db"

    if replace:
        os.remove(f"data/{my_file}")

    if os.path.exists(f"data/{my_file}"):
        message = f"A database named '{my_file}' already exists. Do you want to replace it. Yes (y) or No (n)?"
        user_input = input(message)
        if user_input == "y":
            os.remove(f"data/{my_file}")
            replace = True

    if replace:
        my_engine = initialize_engine(my_file)
        initialize_tables(my_engine)
        accounts = ask_user()
        if accounts:
            for account in accounts.items():
                add_account(my_engine, account[0], account[1])


def add_account(engine,account_name,password):
    user = Account(account_name=account_name, password=password)
    user.activity = Activity(account_id=user.id)
    with Session(engine) as session:
        session.add(user)
        session.commit()


def update_activity(engine,account_name,last_login=None,api_limit_hit=None,api_limit_reset=None):
    with Session(engine) as session:
        account = session.query(Account).filter_by(account_name=account_name).first()
        if account:
            if last_login:
                account.activity.last_login = last_login
            if api_limit_hit:
                account.activity.api_limit_hit = api_limit_hit
            if api_limit_reset:
                account.activity.api_limit_reset = api_limit_reset
            session.commit()
        else:
            pass


def add_history(engine,account_id,model,message,completion):
    chats = Chats(account_id=account_id, model=model,message=message,completion=completion)
    with Session(engine) as session:
        session.add(chats)
        session.commit()


def get_random_account(engine):
    try:
        with Session(engine) as session:
            account, activity = session.query(Account, Activity) \
                .join(Activity) \
                .filter(((Activity.api_limit_reset.is_(None)) | (datetime.now() > Activity.api_limit_reset))) \
                .order_by(func.random()).first()

        return {"success_flag":True, "query":(account, activity)}
    except:
        return {"success_flag":False}






