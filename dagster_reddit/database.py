from dagster import ConfigurableIOManager, IOManager, OutputContext, InputContext, io_manager, StringSource, \
    InitResourceContext
import pandas as pd
from sqlalchemy import create_engine


class PostgresDataframeIOManager(IOManager):
    def __init__(self, connection_string: str):
        self._connection_string = connection_string

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        if not isinstance(obj, pd.DataFrame):
            return

        table_name = context.asset_key.to_python_identifier()
        engine = create_engine(self._connection_string)
        obj.to_sql(table_name, engine, if_exists='replace', index=False)

        context.add_output_metadata({'table_name': table_name})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_name = context.asset_key.to_python_identifier()
        engine = create_engine(self._connection_string)

        df = pd.read_sql(f"SELECT * FROM public.{table_name}", engine)
        return df


@io_manager(config_schema=StringSource)
def postgres_io_manager(context: InitResourceContext) -> PostgresDataframeIOManager:
    return PostgresDataframeIOManager(context.resource_config)
