from dataclasses import dataclass


@dataclass
class SqlObject:

    upsert_user_profile_query: str = """
        INSERT INTO user_data
        (user_id, prdcer_dataset, prdcer_lyrs, prdcer_ctlgs, draft_ctlgs)
            VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id) DO UPDATE
        SET prdcer_dataset = $2, 
            prdcer_lyrs = $3, 
            prdcer_ctlgs = $4, 
            draft_ctlgs = $5; 
    """
    load_user_profile_query: str = """SELECT * FROM user_data WHERE user_id = $1;"""

    population_w_city: str = """SELECT * FROM "schema_marketplace".population
                                    where "Location" = $1;
                                    """
    housing_w_city: str = """SELECT * FROM "schema_marketplace".housing
                                    where "Location" = $1;
                                    """
    household_w_city: str = """SELECT * FROM "schema_marketplace".household
                                    where "Location" = $1;
                                    """