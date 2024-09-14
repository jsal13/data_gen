import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import duckdb
import pandas as pd
from attrs import asdict, define
from faker import Faker

DEFAULT_CUSTOMER_NUMBER = 100
DEFAULT_LOANS_MAX_NUMBER = 5 * DEFAULT_CUSTOMER_NUMBER  # 5 from the num loans each.
LOAN_STATUSES = ["GREEN", "YELLOW", "RED"]


@define
class LoanInfo:
    """Daily record of a loan's info, status, values, etc."""

    # ids
    loan_id: int
    loan_status_id: int
    lender_id: int

    # dates
    recorded_on: datetime
    followup_on: date
    previous_payment_on: date
    next_payment_on: date

    # numerics
    due_interest_amount: Decimal
    due_principal_amount: Decimal
    due_escrow_amount: Decimal
    due_fees_amount: Decimal
    previous_payment_amount: Decimal
    next_payment_amount: Decimal
    credit_limit_amount: Decimal
    number_of_unique_delinquencies: int


@define
class Customer:
    """Customer Information."""

    # ids
    customer_id: int
    # strings
    name: str
    address: str
    ssn: str
    # numerics
    age: int


@define
class LoanStatus:
    """Lookup for Loan Statuses."""

    loan_status_id: int
    loan_status_value: str


@define
class Lender:
    """Lookup for Lenders."""

    lender_id: int
    lender_name: str


@define
class CustomerLoan:
    """Lookup for a customer-loan pairing."""

    loan_id: int
    customer_id: int


def generate_loan_info_row(loan_id: int) -> dict[str, Any]:
    """Generate a random LoanInfo object."""
    fake = Faker()

    recorded = fake.date_time()
    next_payment_on = fake.date_between_dates(
        recorded + timedelta(days=10), datetime.now(UTC)
    )
    previous_payment_on = fake.date_between_dates(
        recorded - timedelta(days=random.randint(7, 70)), recorded
    )
    followup_on = fake.date_between_dates(previous_payment_on, next_payment_on)

    return asdict(
        LoanInfo(
            loan_id=loan_id,
            loan_status_id=random.randint(0, 2),
            lender_id=random.randint(0, 5),
            # dates
            recorded_on=recorded,
            followup_on=followup_on,
            previous_payment_on=previous_payment_on,
            next_payment_on=next_payment_on,
            # numerics
            due_interest_amount=round(10 ** random.randint(1, 5) * random.random(), 2),
            due_principal_amount=round(10 ** random.randint(1, 5) * random.random(), 2),
            due_escrow_amount=round(10 ** random.randint(1, 5) * random.random(), 2),
            due_fees_amount=round(10 ** random.randint(1, 5) * random.random(), 2),
            previous_payment_amount=round(
                10 ** random.randint(1, 5) * random.random(), 2
            ),
            next_payment_amount=round(10 ** random.randint(2, 4) * random.random(), 2),
            credit_limit_amount=round(10 ** random.randint(3, 5) * random.random(), 0),
            number_of_unique_delinquencies=random.randint(0, 10),
        )
    )


def generate_loan_info_rows(n: int = DEFAULT_LOANS_MAX_NUMBER) -> list[dict[str, Any]]:
    """Generate many LoanInfo rows."""
    return [generate_loan_info_row(loan_id=idx) for idx in range(0, n + 1)]


def generate_customer(customer_id: int) -> dict[str, Any]:
    """Generate a random LoanInfo object."""
    fake = Faker()

    return asdict(
        Customer(
            customer_id=customer_id,
            name=fake.name(),
            address=fake.address().replace("\n", " "),
            ssn=fake.ssn(),
            age=random.randint(21, 100),
        )
    )


def generate_customers(n: int = DEFAULT_CUSTOMER_NUMBER) -> list[dict[str, Any]]:
    """Generate many Customer objects."""
    return [generate_customer(customer_id=idx) for idx in range(0, n + 1)]


def associate_loans_with_customers(
    num_customers: int = DEFAULT_CUSTOMER_NUMBER,
    num_loans: int = DEFAULT_LOANS_MAX_NUMBER,
) -> list[dict[str, Any]]:
    """Associates Customers with Loans."""
    loan_ids = list(range(num_loans))

    loan_customer_list: list[dict[str, Any]] = []
    for customer_id in range(num_customers):
        num_loans_per_current_customer = random.choices(
            [1, 2, 3, 4, 5], weights=[0.7, 0.2, 0.05, 0.04, 0.01]
        )[0]
        for _ in range(num_loans_per_current_customer):
            loan_id = loan_ids.pop(random.randint(0, len(loan_ids) - 1))
            loan_customer_list.append(
                asdict(CustomerLoan(loan_id=loan_id, customer_id=customer_id))
            )

    return loan_customer_list


def generate_lenders(num_lenders: int = 5) -> list[dict[str, Any]]:
    """Generate random companies to be a lender."""
    fake = Faker()
    return [
        asdict(Lender(lender_id=idx, lender_name=fake.company()))
        for idx in range(num_lenders)
    ]


def generate_loan_statuses() -> list[dict[str, Any]]:
    """Generate list of `LoanStatus`es."""
    return [
        asdict(LoanStatus(loan_status_id=n, loan_status_value=value))
        for n, value in enumerate(LOAN_STATUSES)
    ]


def generate_all() -> dict[str, pd.DataFrame]:
    """Generate a dict of dataframes for the db."""
    customers = generate_customers()
    loan_info = generate_loan_info_rows()
    lenders = generate_lenders()
    statuses = generate_loan_statuses()
    loan_customer_list = associate_loans_with_customers()

    return {
        "customers": pd.DataFrame(customers),
        "loan_info": pd.DataFrame(loan_info),
        "lenders": pd.DataFrame(lenders),
        "statuses": pd.DataFrame(statuses),
        "loan_customer_lookup": pd.DataFrame(loan_customer_list)
    }


def dump_to_csv(payload: dict[str, pd.DataFrame]) -> None:
    """Dump everything to CSV."""
    for val, df in payload.items():
        df.to_csv(f"./data/{val}.csv", index=False)

def dump_to_duckdb(payload: dict[str, pd.DataFrame]) -> None:
    """Dump everything into DuckDB."""
    with duckdb.connect("./database.db") as con:
        for val, df in payload.items():  # noqa: B007
            con.sql(f"CREATE TABLE {val} AS SELECT * FROM df")

if __name__ == "__main__":
    payload = generate_all()
    dump_to_duckdb(payload=payload)
