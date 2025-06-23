bash_use_variable_good = BashOperator(
    task_id="bash_use_variable_good",
    bash_command="echo variable foo=${foo_env}",
    env={"foo_env": "{{ var.value.get('foo') }}"},
)


@task
def my_task():
    # this is fine, because func my_task called only run task, not scan DAGs.
    var = Variable.get("foo")
    print(var)
