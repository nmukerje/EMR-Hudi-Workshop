aws ecs run-task \
        --cluster fargate-cluster \
        --task-definition kinesis-producer \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={subnets=['subnet-0407b1173827ed96a'],securityGroups=['sg-05b84f422b01057df'],assignPublicIp='ENABLED'}"