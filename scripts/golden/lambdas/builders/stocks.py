from .builder_interface import BuilderInterface
from io import StringIO
from pandas import read_csv, DataFrame


class Stocks(BuilderInterface):

    def __init__(self, source_bucket, source_key):
        print('__init__ stocks')
        super().__init__()
        print('stocks super created')
        self.source_bucket = source_bucket
        self.source_key = source_key

    def get_sources(self):
        """Return dictionary with sources to build stocks tables"""
        object_content = self.extract_data(self.source_bucket, self.source_key)
        print('Objeto extraido')
        print(object_content)
        df_portfolio = read_csv(StringIO(object_content))
        print('Dataframe creado')
        portfolio_columns = df_portfolio.columns
        portfolio_metadata = self.source_key.split('/')[-1]
        print('Returning sources')

        return {
            'df_portfolio': df_portfolio,
            'portfolio_columns': portfolio_columns,
            'portfolio_metadata': portfolio_metadata
        }

    def _set_broker_name(self, broker_name):
        """Set broker name"""
        match broker_name:
            case 'bullma':
                return 'Bull Market'
            case 'iol':
                return 'Invertir Online'
            case _:
                return 'Unknown Broker'

    def build_portfolio_table(self, portfolio_metadata, df_portfolio):
        """Build the portfolio table"""
        portfolio_metadata_list = portfolio_metadata.split('_')
        portfolio_name = self._set_broker_name(portfolio_metadata_list[0])
        portfolio_date = portfolio_metadata_list[-1].replace('.csv', '')
        total_portfolio = df_portfolio['Total'].sum()
        portfolio_data = {
            'broker_name': [portfolio_name], 
            'total_investment': [total_portfolio], 
            'partition_date': [portfolio_date]
        }
        df_portfolio_table = DataFrame(portfolio_data)
        print('Building portfolio table')
        self.load_data(df_portfolio_table, 'golden', 'portfolios')


    def build_tables(self, sources_dict):
        """Build the stocks tables"""
        df_portfolio = sources_dict['df_portfolio']
        # portfolio_columns = sources_dict['portfolio_columns']
        portfolio_metadata = sources_dict['portfolio_metadata']

        self.build_portfolio_table(portfolio_metadata, df_portfolio)
