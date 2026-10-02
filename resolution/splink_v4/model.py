"""Truth-blind model specification. No arbitrary evidence points or scenario rules."""


def comparison(column, fuzzy=False, term_frequency=False):
    levels = [
        {"sql_condition": f"{column}_l IS NULL OR {column}_r IS NULL",
         "label_for_charts": "Missing", "is_null_level": True},
        {"sql_condition": f"{column}_l = {column}_r", "label_for_charts": "Exact"},
    ]
    if term_frequency:
        levels[1]["tf_adjustment_column"] = column
    if fuzzy:
        for threshold in (0.9, 0.75):
            levels.append({
                "sql_condition": f"1.0 - levenshtein({column}_l, {column}_r) / "
                    f"greatest(length({column}_l), length({column}_r), 1) >= {threshold}",
                "label_for_charts": f"Normalised edit similarity >= {threshold}",
            })
    levels.append({"sql_condition": "ELSE", "label_for_charts": "Disagree"})
    return {"output_column_name": column, "comparison_levels": levels}


def settings(prior=0.0001, max_iterations=25):
    from splink import SettingsCreator
    # Full name is compared once. Address includes postcode, so postcode is not
    # separately counted as independent evidence. Missing is neutral.
    return SettingsCreator(
        link_type="dedupe_only", unique_id_column_name="record_key",
        probability_two_random_records_match=prior,
        max_iterations=max_iterations, em_convergence=0.001,
        retain_intermediate_calculation_columns=True,
        retain_matching_columns=True,
        comparisons=[
            comparison("name_n", fuzzy=True, term_frequency=True),
            comparison("personal_email_n", term_frequency=True),
            comparison("personal_phone_n", term_frequency=True),
            comparison("dob_usable"),
            comparison("home_address_n", fuzzy=True, term_frequency=True),
        ],
        blocking_rules_to_generate_predictions=[
            "l.personal_email_n = r.personal_email_n",
            "l.personal_phone_n = r.personal_phone_n",
            "l.family_name_n = r.family_name_n AND l.dob_usable = r.dob_usable",
            "l.name_n = r.name_n AND l.postcode_n = r.postcode_n",
        ],
    )


def train(linker, max_pairs=1_000_000):
    linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs, seed=20260921)
    # Independently selected training blocks: the blocked comparison is held
    # out of that EM session. No truth or curated scenario labels are accessed.
    for rule in (
        "l.personal_email_n = r.personal_email_n",
        "l.personal_phone_n = r.personal_phone_n",
        "l.name_n = r.name_n AND l.dob_usable = r.dob_usable",
    ):
        linker.training.estimate_parameters_using_expectation_maximisation(rule)
