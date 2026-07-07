import pandas as pd
import numpy as np

class Report:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def dataframe_checks(self):
        # check whether the dataframe is a pandas dataframe
        # you will need to check whether the data frame is of the right format first
        if not isinstance(self.dataframe, pd.DataFrame):
            raise TypeError("Pandas dataframe required")
        # check the dataframe is of the right format
        expected_keys = ['question', 'yes_6mo', 'no_6mo', 'yes_3mo', 'no_3mo', 'yes_1mo',
       'no_1mo', 'yes_1wk', 'no_1wk', 'yes_1day', 'no_1day', 'yes_resolution',
       'no_resolution', 'category', 'outcome', 'condition_id',
       'resolution_date', 'missing_horizons']
        if list(self.dataframe.columns) != expected_keys:
            raise ValueError("dataframe is not in the correct format")

    # I am going to need a general case function that creates a new dataframe given a condition
    def split_df_one_condition(self, condition):

        '''
        This has a big issue currently, that it is called constantly creating a new dataframe each time
        '''
        #use dataframe checks
        self.dataframe_checks()

        options = {
            "six_month": "yes_6mo",
            "three_month": "yes_3mo",
            "one_month": "yes_1mo",
            "one_week": "yes_1wk",
            "one_day": "yes_1day"
        }
        # check time frame is in options
        if condition not in options.keys():
            raise ValueError("time frame not in dataset")

        questions_data = []
        month_data = []
        result_data = []
        split_df = {
            "question": list(),
            "time frame": list(),
            "result": list()
        }
        for idx, value in enumerate(self.dataframe[options[condition]]):
            if pd.isna(value):
                continue
            else:
                month_data.append(value)
                questions_data.append(self.dataframe["question"][idx])
                result_data.append(self.dataframe["outcome"][idx])

        split_df["question"] = questions_data
        split_df["time frame"] = month_data
        split_df["result"] = result_data
        split_df = pd.DataFrame(split_df)

        return split_df

    def freq_report(self, timeframe, prob, margin):
        dataframe = self.split_df_one_condition(timeframe)

        #if the probability in "time frame" is prob within margin
        #what percentage of outcomes for these are 1

        markets = []
        results = []
        for idx, value in enumerate(dataframe["time frame"]):
            if np.isclose(value, prob, atol = margin):
                markets.append(value)
                results.append(dataframe['result'][idx])
            else:
                continue

        try:
            frequency = sum(results) / len(markets)
        except ZeroDivisionError:
            frequency = np.nan  # no markets in this price bucket -> undefined

        return len(markets), sum(results), frequency

    def report_table(self, metric = "count", step = 0.1, margin = 0.05):
        #check that the metric is ok
        valid_metrics = ("count", "resolution", "freq")
        if metric not in valid_metrics:
            raise ValueError("metric not in valid metrics")
        else:
            pass

        table = {
            "Price": list(),
            "six_month": list(),
            "three_month": list(),
            "one_month": list(),
            "one_week": list(),
            "one_day": list()
        }

        #create the prices based on the step and the margin
        price_list = []
        index = 0
        while index <= 1:
            price_list.append(np.round(index, decimals = 3))
            index += step

        table["Price"] = price_list

        market_keys = list(table.keys())
        market_keys.remove("Price")
        for market in market_keys:
            market_list = []
            for price in price_list:
                if metric == "count":
                    market_list.append(self.freq_report(market, price, margin)[0])
                elif metric == "resolution":
                    market_list.append(self.freq_report(market, price, margin)[1])
                else:
                    market_list.append(self.freq_report(market, price, margin)[2])

            table[market] = market_list

        return pd.DataFrame(table)