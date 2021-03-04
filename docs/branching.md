# Branching strategy

There are two main branches in this repository; main and develop. These are both protected branches and should never be altered directly.

The branching strategy is outlined below

![branching](/docs/images/branching.png)

## Feature branches

To build functionality, use feature branches. Feature branches should be squash merged into develop after at least one approval.

Branch naming: `feature/*`

## Bugfix brances

To fix bugs in develop, use bugfix branches. Squash merge into develop after at least one approval.

Branch naming: `bugfix/*`

## Hotfix brances

On rare occations, it may be necessary to do quick fixes directly into main. In this case use hotfix branches and merge directly into main. After merging into main, make sure to merge main into develop to.

Branch naming: `hotfix/*`
