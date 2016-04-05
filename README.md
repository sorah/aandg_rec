# aandg_rec

## Features

- Record [JOQR 超！A&G](http://www.agqr.jp) continuously
- Store in Amazon S3
- High availability, run multiple instances for backup

## Usage

After configuration, run

```
$ bundle install
$ bundle exec ./rec_all.rb
```

or

```
$ docker run quay.io/sorah/aandg_rec:latest
```

## Configuration

Place YAML file at `config.yml` or via environment variables:

- `$AGQR_RECORD_DIR` (`record_dir`) default=`./recorded`
- `$AGQR_LOG_DIR` (`log`) default=`./log`
- `$AGQR_S3_REGION` (`s3_region`)
- `$AGQR_S3_BUCKET` (`s3_bucket`)
- `$AGQR_S3_PREFIX` (`s3_prefix`)
- `$AGQR_URL_BASE` (`http_base`) default=`http://localhost`
- `$AGQR_MARGIN_BEFORE` (`margin_before`) in seconds; default=`12`
- `$AGQR_MARGIN_AFTER` (`margin_after`) in seconds; default=`20`
- `$AGQR_EARLY_EXIT_ALLOWANCE` (`allow_early_exit`) in seconds; default=`10`
- `$AGQR_HOSTNAME` default=system hostname (= `hostname(1)`)
