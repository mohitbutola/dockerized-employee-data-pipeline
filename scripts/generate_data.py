from faker import Faker
import random
import csv
from datetime import datetime, timedelta

fake = Faker()

NUM_RECORDS = 1000

def random_salary():
    salary = random.randint(30000, 120000)
    if random.random() < 0.4:
        return f"${salary:,}"
    return str(salary)

def random_email(first, last):
    if random.random() < 0.2:
        return f"{first}.{last}@company"   # invalid
    email = f"{first}.{last}@company.com"
    if random.random() < 0.5:
        email = email.upper()
    return email

def address_formated():
    address = fake.address().replace("\n", "") if random.random() > 0.2 else None
    if address is not None:
        return address if address[0] != '"' else 'India'
    return address
    

with open("data/employees_raw.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "employee_id","first_name","last_name","email","hire_date",
        "job_title","department","salary","manager_id","address",
        "city","state","zip_code","birth_date","status"
    ])

    for i in range(NUM_RECORDS):
        employee_id = random.randint(1000, 1600)  # duplicates by design
        first = fake.first_name()
        last = fake.last_name()

        hire_date = fake.date_between(start_date="-10y", end_date="+2y")  # future dates

        birth_date = fake.date_between(start_date="-60y", end_date="-20y")

        
        job_title = fake.job().replace(",", "").replace("\n", "").replace(' ', '-')  # quotes around job titles

        writer.writerow([
            employee_id,
            first.lower() if random.random() < 0.5 else first,
            last.upper() if random.random() < 0.5 else last,
            random_email(first, last),
            hire_date,
            job_title,
            fake.random_element(["IT", "HR", "Analytics", "Finance", "it"]),
            random_salary(),   # keep messy on purpose
            random.choice([None, random.randint(1000, 1010)]),
            address_formated(),
            fake.city().replace(",", "").replace("\n", "").replace(' ', '-'),
            fake.state_abbr().replace(",", "").replace("\n", "").replace(' ', '-'),
            fake.postcode(),
            birth_date,
            random.choice(["Active", "ACTIVE", "inactive"])
        ])
