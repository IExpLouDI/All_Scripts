from all_params import GetDataSql, check_type
import pandas as pd


if __name__ == '__main__':

    login = 'infectdata'
    password = 'infectdata'

    file_name_excel = 'excel_files/file_name.xlsx'
    file_name_csv = None

    db_connect = GetDataSql(login, password)

    if file_name_csv:
        # Необходимо указать разделить данных в файле
        df = pd.read_csv(file_name_csv, delimiter=',', encoding='utf-8')

        # вставить название справочника
        cls_name = 'cls_selection'
        columns = list(df.columns)

        db_req = f"select max(global_id) + 1 from {cls_name}"
        max_id = db_connect.get_data_test(db_req)[0][0]

        with open('insert_cls_selection.txt', 'w', encoding='utf-8') as f:

            f.write(f'insert into {cls_name}({", ".join(columns)})\nvalues\n')

            for i in range(len(df)):
                temp_str = []

                for column in columns:
                    if column == 'global_id' or column == 'id':
                        temp_str.append(f'{max_id}')
                        max_id += 1

                    else:
                        if pd.notna(df.iloc[i][column]):
                            temp_str.append(check_type(str(df.iloc[i][column])))
                        else:
                            temp_str.append('null')

                if len(df) - 1 == i:
                    f.write(f"""({", ".join(temp_str).replace("__", "'")});""")
                else:
                    f.write(f"""({", ".join(temp_str).replace("__", "'")}),\n""")

                max_id += 1

    elif file_name_excel:
