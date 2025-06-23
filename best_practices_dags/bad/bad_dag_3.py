from airflow.models import Variable

foo_var = Variable.get("foo")
bash_use_variable_bad_1 = BashOperator(
    task_id="bash_use_variable_bad_1", bash_command="echo variable foo=${foo_env}", env={"foo_env": foo_var}
)

bash_use_variable_bad_2 = BashOperator(
    task_id="bash_use_variable_bad_2",
    bash_command=f"echo variable foo=${Variable.get('foo')}",
)

bash_use_variable_bad_3 = BashOperator(
    task_id="bash_use_variable_bad_3",
    bash_command="echo variable foo=${foo_env}",
    env={"foo_env": Variable.get("foo")},
)
