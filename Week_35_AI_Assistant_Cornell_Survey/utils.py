import pandas as pd
from typing import Union, Tuple, List, Optional


def _calculate_single_choice_question_stats(df: pd.DataFrame, question_id: int, *, precision: int = 2):
    """
    Calculates statistics for a single-choice or clusterized open-ended question.

    Args:
        df (pd.DataFrame): DataFrame containing survey data.
        question_id (int): ID of the question to analyze.
        precision (int, optional): Number of decimal points to round to. Default is 2.

    Returns:
        pd.DataFrame: DataFrame with statistics:
            - `weighted_percentage`: the percentage of respondents who selected the option (considering respondent weights);
            - `weighted`: the sum of respondent weights who selected the option;
            - `count_percentage`: the percentage of respondents who selected the option (without considering weights);
            - `count`: the total number of respondents who selected the option.
    """
    question_data = df.loc[:, df.columns.str.startswith(f"[{question_id}]")]

    column = question_data.columns[0]
    print("Question:", column[column.find("]") + 2 :])
    print("Question type: single-choice or clusterized open-ended")

    question_data.columns = ["option"]

    question_data = question_data.dropna(how="all", axis="index")
    question_data = question_data.merge(df["Weight"], how="left", left_index=True, right_index=True)

    print(f"Number of respondents: {len(question_data)}")
    print(f'Weighted number of respondents: {round(question_data["Weight"].sum(), precision)}')

    question_stats = question_data.groupby("option").agg(count=("option", "count"), weighted=("Weight", "sum"))

    question_stats["count_percentage"] = question_stats["count"] / len(question_data) * 100
    question_stats["weighted_percentage"] = question_stats["weighted"] / question_data["Weight"].sum() * 100

    question_stats = question_stats[reversed(sorted(question_stats.columns))]

    question_stats = question_stats.sort_values(by=["weighted_percentage"], ascending=False)
    question_stats = question_stats

    return question_stats.round(precision)


def _calculate_multi_choice_question_stats(df: pd.DataFrame, question_id: int, *, precision: int = 2) -> pd.DataFrame:
    """
    Calculates statistics for a multi-choice question.

    Args:
        df (pd.DataFrame): DataFrame containing survey data.
        question_id (int): ID of the question to analyze.
        precision (int, optional): Number of decimal points to round to. Default is 2.

    Returns:
        pd.DataFrame: DataFrame with statistics:
            - `weighted_percentage`: the percentage of respondents who selected the option (considering respondent weights);
            - `weighted`: the sum of respondent weights who selected the option;
            - `count_percentage`: the percentage of respondents who selected the option (without considering weights);
            - `count`: the total number of respondents who selected the option.
    """
    question_data = df.loc[:, df.columns.str.startswith(f"[{question_id}]")]

    column = question_data.columns[0]
    print("Question:", column[column.find("]") + 2 : column.find(":") - 1])
    print("Question type: multi-choice")

    question_data = question_data.rename(columns=lambda value: value[value.find(":") + 2 :])

    question_data = question_data.dropna(how="all", axis="index")
    question_data = question_data.merge(df["Weight"], how="left", left_index=True, right_index=True)

    print(f"Number of respondents: {len(question_data)}")
    print(f'Weighted number of respondents: {round(question_data["Weight"].sum(), precision)}')

    question_stats = question_data.apply(
        lambda column: pd.Series(
            data=[column.notna().sum(), question_data.loc[column.notna(), "Weight"].sum()], index=["count", "weighted"]
        )
    )

    question_stats = question_stats.T

    question_stats["count_percentage"] = question_stats["count"] / len(question_data) * 100
    question_stats["weighted_percentage"] = question_stats["weighted"] / question_data["Weight"].sum() * 100

    question_stats.index.name = "option"
    question_stats = question_stats[reversed(sorted(question_stats.columns))]

    question_stats = question_stats.sort_values(by=["weighted_percentage"], ascending=False)
    question_stats = question_stats.drop(index=["Weight"])

    return question_stats.round(precision)


def _calculate_matrix_question_stats(
    df: pd.DataFrame,
    question_id: int,
    *,
    precision: int = 2,
    value_order: Optional[List] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Calculates statistics for a matrix question.

    Args:
        df (pd.DataFrame): DataFrame containing survey data.
        question_id (int): ID of the question to analyze.
        precision (int, optional): Number of decimal points to round to. Default is 2.
        value_order (List, optional): Predefined order for the values (rows) of the matrix.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple of four DataFrames for each metric:
            - `weighted_percentage`: the percentage of respondents who selected the option (considering respondent weights);
            - `weighted`: the sum of respondent weights who selected the option;
            - `count_percentage`: the percentage of respondents who selected the option (without considering weights);
            - `count`: the total number of respondents who selected the option.
    """
    question_data = df.loc[:, df.columns.str.startswith(f"[{question_id}]")]

    column = question_data.columns[0]
    print("Question:", column[column.find("]") + 2 : column.find(":") - 1])
    print("Question type: matrix")

    question_data = question_data.rename(columns=lambda value: value[value.find(":") + 2 :])
    option_columns = question_data.columns.to_list()

    question_data = question_data.dropna(how="all", axis="index")
    question_data = question_data.merge(df["Weight"], how="left", left_index=True, right_index=True)

    print(f"Total number of respondents: {len(question_data)}")
    print(f'Total weighted number of respondents: {round(question_data["Weight"].sum(), precision)}')

    question_stats_lambdas = (
        # weighted percetange
        lambda column: (
            question_data.groupby(column)["Weight"].sum() / question_data.loc[column.notna(), "Weight"].sum() * 100
        ),
        # weighted
        lambda column: (question_data.groupby(column)["Weight"].sum()),
        # count percentage
        lambda column: (column.value_counts(normalize=True) * 100),
        # count
        lambda column: column.value_counts(),
    )

    result = []
    for question_stats_lambda in question_stats_lambdas:
        question_stats = question_data[option_columns].apply(question_stats_lambda)

        question_stats.index.name = "option"

        if value_order is not None:
            question_stats = question_stats.loc[value_order]

        question_stats = question_stats[
            sorted(question_stats.columns, key=lambda column: tuple(question_stats[column]), reverse=True)
        ]

        result.append(question_stats.round(precision))

    return tuple(result)


def calculate_question_stats(
    df: pd.DataFrame,
    question_id: int,
    *,
    precision: int = 2,
    value_order: Optional[List] = None,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    """
    Calculates question statistics based on its type (single-choice, multi-choice, or matrix).

    Args:
        df (pd.DataFrame): DataFrame containing survey data.
        question_id (int): ID of the question to analyze.
        precision (int, optional): Number of decimal points to round to. Default is 2.
        value_order (List, optional): Predefined order for the values (rows) of the matrix. Only used for matrix questions.

    Returns:
        Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
            - DataFrame for single or multi-choice question statistics.
            - A tuple of DataFrames for matrix question statistics.
    """
    question_data = df.loc[:, df.columns.str.startswith(f"[{question_id}]")]

    if len(question_data.columns) == 1:
        return _calculate_single_choice_question_stats(df, question_id, precision=precision)

    option = question_data.columns[0].split(" : ")[-1]

    if (question_data.iloc[:, 0] == option).sum() == (question_data.iloc[:, 0].notna()).sum():
        return _calculate_multi_choice_question_stats(df, question_id, precision=precision)

    return _calculate_matrix_question_stats(df, question_id, precision=precision, value_order=value_order)


def mask(df: pd.DataFrame, question_id: int, *, option: Optional[str] = None, value: Optional[str] = None) -> pd.Series:
    """
    Creates a mask for filtering data rows based on a question's option and/or value.

    For single-choice or clusterized open-ended questions, a value is required.
    For multi-choice questions, an option is required.
    For matrix questions, both an option and a value are required.

    Args:
        df (pd.DataFrame): DataFrame containing survey data.
        question_id (int): ID of the question to create the mask for.
        option (str, optional): The specific option to filter by.
        value (str, optional): The specific value to filter by.

    Raises:
        ValueError: If required arguments for the question type are not provided.

    Returns:
        pd.Series: Boolean Series to be used as a filter on rows of the DataFrame.
    """
    question_data = df.loc[:, df.columns.str.startswith(f"[{question_id}]")]

    if len(question_data.columns) == 1:
        if not (value is not None and option is None):
            raise ValueError("You should specify 'value'")

        return question_data.squeeze() == value

    question_data = question_data.rename(columns=lambda value: value[value.find(":") + 2 :])

    if (question_data.iloc[:, 0] == question_data.columns[0]).sum() == (question_data.iloc[:, 0].notna()).sum():
        if not (value is None and option is not None):
            raise ValueError("You should specify 'option'")

        return question_data[option].notna()

    if not (option is not None and value is not None):
        raise ValueError("You should specify 'option' and 'value'")

    return question_data[option] == value
