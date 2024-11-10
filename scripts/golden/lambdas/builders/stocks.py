from .builder_interface import BuilderInterface
from io import StringIO
from pandas import read_csv, DataFrame, read_sql
from sqlalchemy import text

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
    
    def _get_securitie_type(self, name):
        name = name.lower().split(' ')
        keywords = {
            'cedear': 'cedear',
            'on': 'obligacion negociable',
            'obligacion': 'obligacion negociable',
            'bon': 'bono',
            'bono': 'bono',
            'bonos': 'bono',
            'vto': 'bono',
            'letra': 'letra del tesoro',
            'fondo': 'fondo comun de inversion',
            'fci': 'fondo comun de inversion',
            'comun': 'fondo comun de inversion'
        }

        for key, value in keywords.items():
            if key in name:
                return value
        return 'accion'
    
    def _get_financial_instrument_type(self, securitie_type):
        securitie_type = securitie_type.lower()

        if securitie_type in ['cedear', 'accion', 'fondo comun de inversion']:
            return 'renta variable'
        elif securitie_type in ['bono', 'obligacion negociable', 'letra del tesoro']:
            return 'renta fija'
        else:
            return 'desconocido'

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
        if self._check_portfolio_in_table(portfolio_name, portfolio_date) is None:
            df_portfolio_table = DataFrame(portfolio_data)
            print('Building portfolio table')
            self.load_data(df_portfolio_table, 'golden', 'portfolios')
            print('Portfolio table built')
        else:
            print('Portfolio already in table')

    def _check_securities_in_table(self, tickets_list):
        """Get the securities table"""
        query_to_check = \
            f"SELECT * FROM golden.securities WHERE ticket IN {tuple(tickets_list)}"
        with self.engine.connect() as conn:
            df_securities = read_sql(query_to_check, conn)
            print('Dataframe securities created')
            print(df_securities)

        if df_securities.empty:
            return None
        return df_securities

    def _check_portfolio_in_table(self, broker_name, partition_date):
        """Check if the portfolio is in the table"""
        query_to_check = \
            f"SELECT * FROM golden.portfolios WHERE broker_name = '{broker_name}' AND partition_date = '{partition_date}'"
        with self.engine.connect() as conn:
            df_portfolio = read_sql(query_to_check, conn)
            print('Dataframe portfolio created')
            print(df_portfolio)

        if df_portfolio.empty:
            return None
        return df_portfolio

    def build_securities_table(self, df_portfolio):
        """Build the securities table"""

        df_new_securities = df_portfolio[['Ticket', 'Nombre']]
        df_new_securities.columns = df_new_securities.columns.str.lower()        
        df_new_securities.rename(columns={'nombre': 'full_name'}, inplace=True)
        df_new_securities['securitie_type'] = df_new_securities['full_name'].apply(
            self._get_securitie_type
        )
        df_new_securities['financial_instrument_type'] = df_new_securities['securitie_type'].apply(
            self._get_financial_instrument_type
        )
        df_new_securities['dividend_yield'] = None
        df_new_securities['par_value'] = None

        df_last_securities = self._check_securities_in_table(df_new_securities['ticket'].tolist())

        if df_last_securities is None:
            print('new securities not in table')
        else:
            df_new_securities = df_new_securities.loc[
                ~df_new_securities['ticket'].isin(df_last_securities['ticket'])
            ]
        
        if len(df_new_securities) == 0:
            print('No new securities to load')
        else:
            print('Loading new securities')
            print(df_new_securities)
            self.load_data(df_new_securities, 'golden', 'securities')
            print('New securities loaded')
    
    def _delete_stocks_with_partition_date_portfolio_id(self, partition_date, id_portfolio):
        """Delete stocks with partition_date and id_portfolio"""
        query_to_delete = text(
            "DELETE FROM golden.stocks WHERE partition_date = :partition_date "
            "AND id_portfolio = :id_portfolio"
        ).bindparams(partition_date=partition_date, id_portfolio=int(id_portfolio))
        
        print('Deleting stocks')
        print(query_to_delete)
        with self.engine.connect() as conn:
            result = conn.execute(query_to_delete)
            print(f'{result.rowcount} rows deleted')
            conn.commit()
        print('Stocks deleted')
    
    def build_stocks_tables(self, portfolio_metadata, df_portfolio):
        """Build the stocks tables"""
        portfolio_metadata_list = portfolio_metadata.split('_')
        portfolio_name = self._set_broker_name(portfolio_metadata_list[0])
        portfolio_date = portfolio_metadata_list[-1].replace('.csv', '')

        df_portfolio_in_table = self._check_portfolio_in_table(portfolio_name, portfolio_date)
        df_securities_in_table = self._check_securities_in_table(df_portfolio['Ticket'].tolist())

        df_portfolio['partition_date'] = portfolio_date
        df_portfolio = df_portfolio.merge(df_securities_in_table[['ticket', 'id_securitie']], left_on='Ticket', right_on='ticket', how='left')
        df_portfolio = df_portfolio.merge(df_portfolio_in_table[['id_portfolio', 'partition_date']], left_on='partition_date', right_on='partition_date', how='left')

        del df_portfolio_in_table, df_securities_in_table

        df_portfolio.drop(columns=['Ticket', 'Nombre', 'Total', 'ticket'], inplace=True)

        df_portfolio.rename(columns={'Cantidad': 'quantity', 'Ultimo Precio': 'last_price', 'PPC': 'average_purchase_price'}, inplace=True)

        df_portfolio['profit_loss_percent'] = (
            (df_portfolio['last_price'] - df_portfolio['average_purchase_price']) / df_portfolio['average_purchase_price'] * 100
        ).round(2)

        df_portfolio[['dividend_yield', 'interest_paid', 'amortization_paid']] = 0

        cols = [
            'id_portfolio', 'id_securitie', 'average_purchase_price', 'quantity', 
            'last_price', 'profit_loss_percent', 'dividend_yield', 'interest_paid', 
            'amortization_paid', 'partition_date'
        ]

        df_portfolio = df_portfolio[cols]

        self._delete_stocks_with_partition_date_portfolio_id(df_portfolio['partition_date'].iloc[0], df_portfolio['id_portfolio'].iloc[0])

        self.load_data(df_portfolio, 'golden', 'stocks')
        print('Stocks table loaded')

    
    def build_tables(self, sources_dict):
        """Build the stocks tables"""
        df_portfolio = sources_dict['df_portfolio']
        # portfolio_columns = sources_dict['portfolio_columns']
        portfolio_metadata = sources_dict['portfolio_metadata']

        self.build_portfolio_table(portfolio_metadata, df_portfolio)
        self.build_securities_table(df_portfolio)
        self.build_stocks_tables(portfolio_metadata, df_portfolio)
