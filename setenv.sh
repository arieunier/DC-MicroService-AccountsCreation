APPNAME=$1
export PYTHONPATH=.:./libs/:./appsrc/:./pyutils
# logs
export LOG_LEVEL=DEBUG

export KAFKA_CLIENT_CERT=`heroku config:get KAFKA_CLIENT_CERT  --app $APPNAME`
export KAFKA_CLIENT_CERT_KEY=`heroku config:get KAFKA_CLIENT_CERT_KEY  --app $APPNAME`
export KAFKA_PREFIX=`heroku config:get KAFKA_PREFIX  --app $APPNAME`
export KAFKA_TRUSTED_CERT=`heroku config:get KAFKA_TRUSTED_CERT  --app $APPNAME`
export KAFKA_URL=`heroku config:get KAFKA_URL  --app $APPNAME`

export PASSWORD=`heroku config:get PASSWORD  --app $APPNAME`
export TOPICS=`heroku config:get TOPICS  --app $APPNAME`
export USERNAME=`heroku config:get USERNAME  --app $APPNAME`
export CONSUMER_KEY=`heroku config:get CONSUMER_KEY  --app $APPNAME`
export CONSUMER_SECRET=`heroku config:get CONSUMER_SECRET --app $APPNAME`
export CLOUDAMQP_APIKEY=`heroku config:get CLOUDAMQP_APIKEY  --app $APPNAME`
export CLOUDAMQP_URL=`heroku config:get CLOUDAMQP_URL  --app $APPNAME`

export JWTKEY=`heroku config:get JWTKEY  --app $APPNAME`
export REDIS_URL=`heroku config:get REDIS_URL  --app $APPNAME`

export DATABASE_URL=`heroku config:get DATABASE_URL  --app $APPNAME`

export BUCKETEER_AWS_ACCESS_KEY_ID=`heroku config:get BUCKETEER_AWS_ACCESS_KEY_ID --app $APPNAME`
export BUCKETEER_AWS_REGION=`heroku config:get BUCKETEER_AWS_REGION --app $APPNAME`
export BUCKETEER_AWS_SECRET_ACCESS_KEY=`heroku config:get BUCKETEER_AWS_SECRET_ACCESS_KEY --app $APPNAME`
export BUCKETEER_BUCKET_NAME=`heroku config:get BUCKETEER_BUCKET_NAME --app $APPNAME`