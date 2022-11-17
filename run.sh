#!/bin/bash -x
BASEDIR=$(dirname $0)
program_name="telequote_java_to_rabbit.py"
cmd="$1"
case "$cmd" in
        start)
                echo "----> STARTING $program_name"
                python3.10 $BASEDIR/$program_name &
                sleep 1
                echo "$program_name running with PID: `pidof python3.10`"
                exit 0
                ;;
        stop)
                echo "STOPING $program_name with PID: `pidof python3.10`"
                kill -9 `pidof python3.10`
                exit 0
                ;;
        status)
                ps -ef | grep "python3.10"
                exit 0
                ;;
        restart)
                echo "RESTARTING $program_name with Old PID: `pidof python3.10`"
                kill -9 `pidof python3.10`
                python3.10 $BASEDIR/$program_name &
                sleep 1
                echo "RESTART Success - New PID: `pidof python3.10`"
                exit 0
                ;;
        *)
                echo "Usage: sh $0 {start|stop|status|restart}"
                exit 1
                ;;
esac