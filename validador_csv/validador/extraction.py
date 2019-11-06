import time
import sys, os
import pandas as pd
import numpy as np
import pandas.io.sql as psql
from sklearn.preprocessing import MinMaxScaler, OrdinalEncoder, OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer, MissingIndicator
from dynamo import TableCreate, DecimalEncoder
from datetime import datetime
import re
import decimal 
import json

def getErrorDesc(info):
    exc_type, exc_obj, exc_tb = info
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    return 'file {}, line {}, cause {}'.format(fname, exc_tb.tb_lineno, exc_type)


class InitDataFrame(object):
    df = pd.DataFrame()
    threshold = 0

    def __init__(self, df, dict_criterion=None, threshold=0):
        """
		data frame manipulation
		:param df: data frame or string table name for load dataframe result
		:param dict_criterion:
		:param threshold: default threshold
		"""
        self.df = df
        self.dict_criterion = dict_criterion
        self.threshold = threshold
        self.table_dinamo_domain = TableCreate('trace_domain')
        self.data_frame_name = 'data_log_process'
        self.table_df = TableCreate(self.data_frame_name)

    def _normalize_columns_name(self, _df=None):
        """
		 set columns names, lower case, remove space, special caracters in columns name
		 change dataframe in instance
		:return:
		"""
        try:
            if _df is not None:
                _df.columns = [a.lower().strip().replace('à', 'a').replace(' - ', '_a_').replace('/', '_').replace(
                        '\'',
                        '_').replace(
                        ' ', '_').replace('(', '').replace(')', '').replace('%', 'percent').replace(',', '') for a in
                               _df.columns]
                return _df
            else:
                self.df.columns = [
                    a.lower().strip().replace('à', 'a').replace(' - ', '_a_').replace('/', '_').replace('\'',
                                                                                                        '_').replace(
                            ' ', '_').replace('(', '').replace(')', '').replace('%', 'percent').replace(',', '') for a
                    in self.df.columns]
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _rename_column(self, new_columns, old_columns):
        try:
            if type(new_columns) is str:
                self.df = self.df.rename(columns={old_columns: new_columns})
            else:
                self.df = self.df.rename(
                        columns=dict(zip(old_columns, new_columns)))
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _strip_column_values(self, column):
        """
		 remove excessive space in values of column
		 change dataframe in instance
		:param column: column name
		:return:
		"""
        try:
            self.df[column] = np.array([a.strip()
                                        for a in self.df[column].values])
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def percentage_nan_missing(self):
        """

		:return: percentange of Non a number values in all data frame
		"""
        try:
            _dict = []
            for item in self.df.columns:
                num_na = round(
                        self.df[item].isnull().sum() / len(self.df[item]), 2)
                _dict.append({item: num_na, 'type': self.df[item].dtypes})
            return _dict
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def percentage_nan_in_column(self, column):
        """

		:return: percentange of zeros values in column
		:param column: column name

		"""
        try:
            return len(list(filter(lambda num: num == np.nan, self.df[column].values))) / len(self.df[column])
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def percentage_zeros_in_column(self, column):
        """

		:return: percentange of zeros values in column
		:param column: column name

		"""
        try:
            return len(list(filter(lambda num: num == 0, self.df[column].values))) / len(self.df[column])
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def percentage_zeros_missing(self):
        """

		:return: percentange of zeros values in all data frame
		"""
        try:
            _dict = []
            for item in self.df.columns:
                _dict.append(
                        {item: len(self.df[self.df[item] == 0]) / len(self.df[item])})
            return _dict

        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _casting_str_decimal(self, columns):
        """
		casting columns values str to decilma
		change dataframe in instance
		:return:
		:param columns: list of column names or name column
		"""
        try:
            if type(columns) is str:
                self.df[columns] = np.array([float(a.replace('.', '').replace(',', '.').replace(
                        "$", "")) if type(a) is str else a for a in self.df[columns].values])
            elif type(columns) is list:
                for item in columns:
                    self.df[item] = np.array([float(a.replace('.', '').replace(',', '.').replace(
                            "$", "")) if type(a) is str else a for a in self.df[item].values])

        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def all_less_than_threshold(self, column, threshold=None):
        """
			:return: all values fo column with value less than threshold
			:param column:  name column
			:param threshold: value threshold
		"""
        try:
            threshold = threshold if threshold is not None else self.threshold
            return list(filter(lambda num: num < threshold, self.df[column].values))

        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _remove_columns_nan_threshold(self, threshold=None):
        """
		remove columns with % of values nan if less than threshold
		change dataframe in instance
		:param threshold: value threshold
		:return:
		"""
        try:
            threshold = threshold if threshold is not None else self.threshold
            dic_missing = self.percentage_nan_missing()
            for item in dic_missing:
                if item[list(item.keys())[0]] >= threshold:
                    self.df.drop(list(item.keys())[0], axis=1, inplace=True)
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _remove_columns_zeros_threshold(self, threshold=None):
        """
		remove columns with % of values zeros if less than threshold
		change dataframe in instance
		:param threshold: value threshold
		:return:
		"""
        try:
            threshold = threshold if threshold is not None else self.threshold
            dic_missing = self.percentage_zeros_missing()
            for item in dic_missing:
                if item[list(item.keys())[0]] >= threshold:
                    self.df.drop(list(item.keys())[0], axis=1, inplace=True)
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _remove_column(self, column, value_r=np.nan):
        self.df = self.df[~self.df[column] == value_r]

    def _log_transformation(self, column):
        self.df = self.df[~self.df[column] <= 0.99]
        self.df[column] = np.log(self.df[column])

    def _remove_row_isnull(self, column):
        self.df = self.df[~self.df[column].isnull()]

    def _min_max_scaler_columns_values(self, column, _feature_range=(0, 1)):
        """
		change dataframe in instance
		:param column: columns name
		:param _feature_range: min max range scaler
		:return:
		"""
        try:
            if self.df[column].max() == _feature_range[1] and self.df[column].min() == _feature_range[0]:
                return 0
            min_max_scaler = MinMaxScaler(feature_range=_feature_range)
            min_max_scaler.fit([self.df[column]])
            self.df[column] = min_max_scaler.fit_transform(
                    pd.DataFrame(self.df[column]))
        except Exception as e:
            print(getErrorDesc(sys.exc_info()), ' - ', column)
            return e.args

    def _encoder_column(self, column, create_dict=False):
        """
		encoder column data frame
		change dataframe in instance
		:param column: column name
		:param create_dict: if true create a domain of enconder value and label in database table (trace_domain), 
		recovery values with query, select * from trace_domain where name = {column name}
		:return:
		"""
        try:
            uniques_labels = self.df[column].unique()
            self.df[column] = self.encoder.fit_transform(
                    self.df[column].values.reshape(-1, 1))
            uniques_encoder = self.df[column].unique()
            _dict = dict(zip(uniques_encoder, uniques_labels))
            if create_dict:
                _result_query = self.table_dinamo_domain.get_info(column)
                if _result_query:
                    print('domain found for {}'.format(column))
                else:
                    response = self.create_dict_dynamo(_dict)
                return _dict
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _custom_encoder_column(self, column, dict_map, create_dict=False):
        """
		encoder column values from dict map
		change dataframe in instance
		:param column: column name
		:param dict_map: dict with value column and value encoder {"value_1":encoder_1, "value_2":encoder_2,...}
		:param create_dict: if true create a domain of enconder value and label in database table (trace_domain), 
		recovery values with query, select * from trace_domain where name = {column name}
		:return:
		"""
        response = False
        try:
            uniques_labels = []
            value_fill_na = 0

            for key, value in dict_map.items():
                uniques_labels.append(key)
                if value == value_fill_na:
                    value_fill_na = value_fill_na - 1
            # uniques_labels = self.df[column].unique()
            # col = self.encoder.fit_transform(self.df[column].values.reshape(-1, 1))
            self.df[column] = self.df[column].map(dict_map)
            self.df[column].fillna(value_fill_na, inplace=True)
            # uniques_encoder = self.df[column].unique()
            dict_map.update({'id': column, 'others': value_fill_na})
            _result_query = self.table_dinamo_domain.get_info(column)
            if _result_query:
                print('domain found for {}'.format(column))
            else:
                response = self.create_dict_dynamo(dict_map)

        except Exception as e:
            print(getErrorDesc(sys.exc_info()))

        return response

    def values_to_column(self, column):
        """
		:param column:
		:return:
		"""
        try:
            onehot_encoder = OneHotEncoder()
            columns_name = self.df[column].unique()
            x = onehot_encoder.fit_transform(
                    self.df[column].values.reshape(-1, 1)).toarray()
            df_new = pd.DataFrame(x, columns=columns_name)
            return self._normalize_columns_name(_df=df_new)
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _impute_missing_values(self, column, missing_values=np.nan, strategy="mean"):
        """
		change dataframe in instance
		:param column: column name
		:param missing_values: missing value (nan, zeros, -1, false...)
		:param strategy: strategy for calc missing value (mean, median most_frequent, constatn)
		:return:
		"""
        try:
            imp = SimpleImputer(missing_values=missing_values, strategy=strategy)
            self.df[column] = imp.fit_transform(
                    pd.DataFrame(self.df[column])).reshape(-1, 1)
        except Exception as e:
            print(getErrorDesc(sys.exc_info()))
            return e.args

    def _replace_nan_with_empty_string(self, column, value_r=''):
        self.df[column] = self.df[column].replace(np.nan, value_r, regex=True)

    def _replace_column_value(self, column, valor_l, value_r=''):
        self.df[column] = self.df[column].replace(np.nan, value_r, regex=True)

    def remove_duplicate_rows(self, subset=None, keep=None):
        if subset:
            return self.df.drop_duplicates(subset=subset, inplace=True)
        elif keep:
            return self.df.drop_duplicates(keep=keep, inplace=True)
        elif keep and subset:
            return self.df.drop_duplicates(subset=subset, keep=keep, inplace=True)

        return self.df.drop_duplicates(inplace=True)

    def to_number(x):
        try:
            _x = x.strip().replace('.', '').replace(',', '.')
            if _x.split('.')[0].isnumeric() and '.' in _x:
                return decimal.Decimal(re.sub(r'[^\d.]', '', _x))
            if x.startswith('R$'):
                _x = x.strip().replace('.', '').replace(',', '.')
                return decimal.Decimal(re.sub(r'[^\d.]', '', _x))
            return x
        except:
            return x

    def data_frame_analytics(self, threshold, file_name_id):
        
        column_item = []
        for item in self.df.columns:
            try:
                #print("_" * 20)
                print("COLUMN ANALYTIC {}".format(item))
                percent_zeros = decimal.Decimal(self.percentage_zeros_in_column(item)*100)
                percent_nan = decimal.Decimal(self.percentage_nan_in_column(item)*100)
                type_colum = str(self.df[item].dtypes)
                try:
                    if len(self.df[item][0].split('/')) > 2:
                        type_colum = 'date'
                except:
                    type_colum = str(self.df[item].dtypes)
                
                print('')                
                
                
                describe = dict(self.df[item].describe())
                uniques_values = []
                if describe.get('unique',11)<10 and type_colum == 'object':
                    uniques_values = [str(i) for i in list(self.df[item].unique())]
                data_ = {'column': item,
                        'type_colum':type_colum,
                        'percent_nan': round(percent_nan,4),
                        'percent_zeros': round(percent_zeros,4), 
                        'describe':describe,
                        'unique_values':uniques_values}
                data_save = json.loads(json.dumps(dict(data_),cls=DecimalEncoder),
                                      parse_float=decimal.Decimal, 
                                      parse_int=decimal.Decimal)
                column_item.append(data_save)                
                                
                print("{} - {} - {}".format(percent_nan, percent_zeros, item))
                print("\n")
            except Exception as e:
                print(getErrorDesc(sys.exc_info()))
                column_item.append({'error_column':item})
                

        #if len(column_item) > 0:
        print(column_item)
        return self.table_df.save({'id':file_name_id,'date':str(datetime.now()),'column_info':column_item})
