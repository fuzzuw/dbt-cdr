#!/usr/bin/python
import psycopg2
from config import config
from dbt.cli.main import dbtRunner, dbtRunnerResult
from contextlib import closing
import time

def main():

    params = config()
    with closing(psycopg2.connect(**params)) as conn:
        with conn.cursor() as cur:

            sql = 'UPDATE dbt_cdr.statuses SET status=2 WHERE status = 1 RETURNING model_name'
            #print(sql)
            cur.execute(sql)
            models = cur.fetchall()
            conn.commit()
            models = [i[0] for i in models]
            print(models)
            
            delimiter = ' '
            str_models = delimiter.join(models)

            dbt = dbtRunner()
            cli_args = ["run", "--select", str_models] #, "--resource-type", "source", "model", "--output", "name"]
            res: dbtRunnerResult = dbt.invoke(cli_args)
            #print(res.success)
            if res.success:
                for r in res.result:
                    print(f"{r.node.name}: {r.status}")

                time.sleep(30)
                delimiter = '\', \''
                str_models = '(\''+delimiter.join(models)+'\')'
                sql = f'''UPDATE dbt_cdr.statuses
                          SET status = COALESCE(status_after,0), 
                              status_after = null
                          WHERE model_name in {str_models}'''
                
                cur.execute(sql)
                print(f'{cur.rowcount} models processed')
                conn.commit()

if __name__ == '__main__':
    main()
