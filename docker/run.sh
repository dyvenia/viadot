IMAGE_TAG=latest
PROFILE="user"

while getopts t: flag
do
    case "${flag}" in
        t) IMAGE_TAG=${OPTARG}
            case ${OPTARG} in 
                dev) PROFILE="dev";;
            esac
        ;;
    esac
done

IMAGE_TAG=$IMAGE_TAG docker-compose --profile $PROFILE up -d --force-recreate

echo ""
echo "Press Enter to exit."

read