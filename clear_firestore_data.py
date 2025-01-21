import asyncio
from backend_common.common_config import CONF
from google.oauth2 import service_account
from firebase_admin import firestore_async
import asyncpg  # For PostgreSQL operations

# Initialize Firestore client
google_auth_creds = service_account.Credentials.from_service_account_file(CONF.firebase_sp_path)
db = firestore_async.AsyncClient(credentials=google_auth_creds)

async def clear_user_data(user_id: str):
    """
    Clears the datasets, layers, and catalogs for a specific user.
    """
    user_ref = db.collection("all_user_profiles").document(user_id)
    user_doc = await user_ref.get()

    if user_doc.exists:
        user_data = user_doc.to_dict()
        if "prdcer" in user_data:
            # Clear the datasets, layers, and catalogs
            user_data["prdcer"] = {
                "prdcer_dataset": {},
                "prdcer_lyrs": {},
                "prdcer_ctlgs": {},
                "draft_ctlgs": {},
            }
            await user_ref.update({"prdcer": user_data["prdcer"]})
            print(f"Cleared data for user: {user_id}")
        else:
            print(f"No 'prdcer' data found for user: {user_id}")
    else:
        print(f"User profile not found for user: {user_id}")

async def clear_all_users_data():
    """
    Clears the datasets, layers, and catalogs for all users.
    """
    users_ref = db.collection("all_user_profiles")
    users_docs = await users_ref.get()

    for user_doc in users_docs:
        user_id = user_doc.id
        await clear_user_data(user_id)

async def clear_fields_in_document(collection_name: str, document_id: str, fields_to_clear: list):
    """
    Clears specific fields in a Firestore document.
    """
    doc_ref = db.collection(collection_name).document(document_id)
    doc = await doc_ref.get()

    if doc.exists:
        update_data = {field: {} for field in fields_to_clear}  # Set fields to empty dictionaries
        await doc_ref.update(update_data)
        print(f"Cleared fields {fields_to_clear} in document {document_id} in collection {collection_name}")
    else:
        print(f"Document {document_id} not found in collection {collection_name}")

async def clear_dataset_and_user_matching():
    """
    Deletes all data inside dataset_matching and user_matching documents 
    in the layer_matching collection.
    """
    # Clear the dataset_matching document
    await db.collection("layer_matchings").document("dataset_matching").set({})
    print("Cleared all data in dataset_matching document")

    # Clear the user_matching document
    await db.collection("layer_matchings").document("user_matching").set({})
    print("Cleared all data in user_matching document")
async def truncate_postgresql_table():
    """
    Truncates the `schema_marketplace.datasets` table in PostgreSQL.
    """
    connection = await asyncpg.connect(
        user="scraper_user",
        password="scraper_password",
        database="dbo_operational",
        host="s-locator.northernacs.com",
        port=5432,
    )

    try:
        await connection.execute('TRUNCATE TABLE "schema_marketplace"."datasets"')
        print("Truncated table schema_marketplace.datasets in PostgreSQL")
    except Exception as e:
        print(f"Error truncating PostgreSQL table: {e}")
    finally:
        await connection.close()

async def main():
    # Clear all user profiles' datasets, layers, and catalogs
    await clear_all_users_data()

    # Clear fields in dataset_matching and user_matching documents
    await clear_dataset_and_user_matching()

    # Truncate the PostgreSQL table
    await truncate_postgresql_table()

if __name__ == "__main__":
    asyncio.run(main())