echo "Cleaning up..."

# Check if there are dangling images
if [ "$(docker images -f "dangling=true" -q)" ]; then
    echo "Removing dangling images..."
    docker rmi $(docker images -f "dangling=true" -q)
fi

docker network prune -f
