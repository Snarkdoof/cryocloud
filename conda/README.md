Given that conda is installed, you may may generate a cryocloud environment with these commands.

```
conda env create -f cc.yml
```

After creation, activate the chosen environment

```
source activate cc
```

To rebuild and environment, deactivate the environment, remove and recreate

```
source deactivate
conda remove --name cc --all
conda env create -f cc.yml
```
