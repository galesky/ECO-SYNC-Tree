# assumes you are on ~/eco-sync-tree folder and already subbed to an instance

rm ./target/PlumtreeOpLogs.jar

mv ../20df41fdf830d180a8ac76eae032255958b99c81/PlumtreeOpLogs.jar ./target

ls -lah ./target | grep PlumtreeOpLogs.jar

sh docker/setupG5k.sh

sh docker/buildImg.sh

cd docker || exit # exit if cd fails

sh loadImage.sh

cd ..

./docker/runExperimentsStable.sh --expname test_vcube_9999999ba63c --nnodes 50 --protocols flood_and_full_membership --payloads 128 --probs 0.5 --nruns 1 --runtime 500 &> log.log &