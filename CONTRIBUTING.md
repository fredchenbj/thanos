# Contributing

When contributing not obvious change to Thanos repository, please first
discuss the change you wish to make via issue or slack, or any other
method with the owners of this repository before making a change.

Please follow the [code of conduct](CODE_OF_CONDUCT.md) in all your interactions with the project.

## Thanos Philosophy

The philosophy of Thanos and our community is borrowing much from UNIX philosophy and the golang programming language.

* Each sub command should do one thing and do it well
  * eg. thanos query proxies incoming calls to known store API endpoints merging the result
* Write components that work together
  * e.g. blocks should be stored in native prometheus format
* Make it easy to read, write, and, run components
  * e.g. reduce complexity in system design and implementation

## Adding New Features / Components

Adding large new features and components to Thanos should be done by first creating a [proposal](docs/proposals) document outlining the design decisions of the change, motivations for the change, and any alternatives that might have been considered.

## Prerequisites

* It is strongly recommended that you use OSX or popular Linux distributions systems e.g. Ubuntu, Redhat, or OpenSUSE for development.

## Pull Request Process

1. Read [getting started docs](docs/getting-started.md) and prepare Thanos.
2. Familiarize yourself with [Makefile](Makefile) commands like `format`, `build`, `proto` and `test`.
3. Fork thanos-io/thanos.git and start development from your own fork. Here are sample steps to setup your development environment:
```console
$ mkdir -p $GOPATH/src/github.com/thanos-io
$ cd $GOPATH/src/github.com/thanos-io
$ git clone https://github.com/<your_github_id>/thanos.git
$ cd thanos
$ git remote add upstream https://github.com/thanos-io/thanos.git
$ git remote update
$ git merge upstream/master
$ make build
$ ./thanos -h
```
4. Keep PRs as small as possible. For each of your PR, you create one branch based on the latest master. Chain them if needed (base PR on other PRs). Here are sample steps you can follow. You can get more details about the workflow from [here](https://gist.github.com/Chaser324/ce0505fbed06b947d962).
```console
$ git checkout master
$ git remote update
$ git merge upstream/master
$ git checkout -b <your_branch_for_new_pr>
$ make build
$ <Iterate your development>
$ git push origin <your_branch_for_new_pr>
```
5. If you don't have a live object store ready add this envvar to skip tests for these:
- THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS

If you skip all of these, the store specific tests will be run against memory object storage only.
CI runs GCS and inmem tests only for now. Not having these variables will produce auth errors against GCS, AWS, Azure or COS tests.

6. If your change affects users (adds or removes feature) consider adding the item to [CHANGELOG](CHANGELOG.md)
7. You may merge the Pull Request in once you have the sign-off of at least one developers with write access, or if you
   do not have permission to do that, you may request the second reviewer to merge it for you.
8. If you feel like your PR waits too long for a review, feel free to ping [`#thanos-prs`](https://slack.cncf.io/) channel on our slack for review!

## Dependency management

The Thanos project uses [Go modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) to manage dependencies on external packages. This requires a working Go environment with version 1.11 or greater and git installed.

To add or update a new dependency, use the `go get` command:

```bash
# Pick the latest tagged release.
go get example.com/some/module/pkg

# Pick a specific version.
go get example.com/some/module/pkg@vX.Y.Z
```

Tidy up the `go.mod` and `go.sum` files:

```bash
make deps
git add go.mod go.sum
git commit
```

You have to commit the changes to `go.mod` and `go.sum` before submitting the pull request.
