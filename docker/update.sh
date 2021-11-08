IMAGE_ID=docker.pkg.github.com/dyvenia/viadot/viadot
IMAGE_TAG=dev


while getopts t: flag
do
    case "${flag}" in
        t) IMAGE_TAG=${OPTARG};;
    esac
done


docker login https://docker.pkg.github.com
docker pull $IMAGE_ID:$IMAGE_TAG
docker tag $IMAGE_ID:$IMAGE_TAG viadot:$IMAGE_TAG
docker image rm $IMAGE_ID:$IMAGE_TAG

docker system prune -f

echo ""
echo "Your viadot image has been successfully updated."
echo "Press Enter to exit."

read
