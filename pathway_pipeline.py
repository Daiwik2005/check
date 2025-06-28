import pathway as pw

# Define schema
class InvoiceSchema(pw.Schema):
    InvoiceID: str
    Date: str
    Amount: float
    Supplier: str
    DueDate: str

# Read CSV in streaming mode
table = pw.io.csv.read(
    "./data/invoices",
    schema=InvoiceSchema,
    mode="streaming",
    autocommit_duration_ms=1000
)

# Define a UDF that prints individual values or a composed message
@pw.udf
def print_invoice(invoice_id, date, amount, supplier, due_date):
    print(f"New Invoice -> ID: {invoice_id}, Date: {date}, Amount: {amount}, Supplier: {supplier}, Due: {due_date}", flush=True)
    return "printed"

# Correct usage: extract fields explicitly
table.select(
    result=print_invoice(
        table.InvoiceID,
        table.Date,
        table.Amount,
        table.Supplier,
        table.DueDate
    )
)

# Run the pipeline
if __name__ == "__main__":
    print("Watching for new invoices...")
    pw.run()
