class Report:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    # I am going to need a general case function that creates a new dataframe given a condition
    def split_df(self, condition):
        options = {
            "six_month": "yes_6mo",
            "three_month": "yes_3mo",
            "one_month": "yes_1mo",
            "one_week": "yes_1wk",
            "one_day": "yes_1day"
        }
        # check time frame is in options
        if condition in options.keys():
            pass
        else:
            return "Error: time frame not in dataset"

        questions_data = []
        month_data = []
        result_data = []
        for i in list(self.dataframe[options[condition]]):
            return i

    def freq_report(self, timeframe, prob, margin):
        pass