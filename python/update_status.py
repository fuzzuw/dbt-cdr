#!/usr/bin/python
import argparse
import psycopg2
from config import config
from dbt.cli.main import dbtRunner, dbtRunnerResult
from contextlib import closing

def main():
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-s", "--source", help="name of the changed source")
    args = argParser.parse_args()

    dbt = dbtRunner()
    cli_args = ["ls", "--select", "source:"+args.source+"+", "--resource-type", "source", "model", "--output", "name"]
    res: dbtRunnerResult = dbt.invoke(cli_args)
    #print(res.success)
    if res.success:
        #print(res.result)
        delimiter = '\', \''
        str_models = '(\''+delimiter.join(res.result)+'\')'
        #sql2 = f'SELECT status FROM dbt_cdr.statuses WHERE model_name in {str_models}'
        sql = f'''UPDATE dbt_cdr.statuses
                  SET status = case when status='NORMAL' then 'OUTDATED'
                                    when status='REFRESHING' then 'REFRESHING_OUTDATED'
                                    when status is NULL then 'OUTDATED'
                                    else status 
                                    end
                  WHERE model_name in {str_models}'''
        #print(sql)

        params = config()
        with closing(psycopg2.connect(**params)) as conn:
            with conn.cursor() as cur:

                cur.execute(sql)
                print(f'{cur.rowcount} rows updated')
                conn.commit()
                
                #cur.execute(sql2)
                #res = cur.fetchall()
                #print(res)


if __name__ == '__main__':
    main()
