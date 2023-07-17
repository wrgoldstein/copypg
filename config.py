small_tables = [
    # "subscriptions",
    "shops", 
    # "features", 
    # "surveys", 
    "questions",
    # "integrations"
]

large_tables = {
    "survey_responses": 0.2,
    "survey_views": 0.05
    }

alterations = [
    # f"""
    #     ALTER TABLE ONLY public.questions
    #     ADD CONSTRAINT questions_pkey PRIMARY KEY (id);
    # """
]

# must be a tuple
shop_ids = ()
