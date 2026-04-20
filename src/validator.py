from typing import Any

import pandas as pd
import great_expectations as gx


def validate_user_signup(event: dict[str, Any]) -> tuple[bool, list[dict]]:
    df = pd.DataFrame([event])
    cols = set(df.columns)

    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_pandas("source")
    asset = datasource.add_dataframe_asset("events")
    batch_request = asset.build_batch_request(dataframe=df)

    suite = context.add_or_update_expectation_suite("user_signup_suite")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="user_signup_suite",
    )

    required_columns = ["user_id", "email", "name", "age", "signup_timestamp"]
    for col in required_columns:
        validator.expect_column_to_exist(col)

    if "user_id" in cols:
        validator.expect_column_values_to_not_be_null("user_id")
    if "email" in cols:
        validator.expect_column_values_to_not_be_null("email")
    if "name" in cols:
        validator.expect_column_values_to_not_be_null("name")
    if "age" in cols:
        validator.expect_column_values_to_be_of_type("age", "int64")

    results = validator.validate()

    violations = []
    for result in results.results:
        if not result.success:
            violations.append(
                {
                    "expectation_type": result.expectation_config.expectation_type,
                    "column": result.expectation_config.kwargs.get("column", "unknown"),
                    "observed_value": str(result.result.get("observed_value", "")),
                }
            )

    return bool(results.success), violations
