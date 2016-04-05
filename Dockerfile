FROM quay.io/sorah/rbenv:2.3
MAINTAINER sorah.jp

RUN flaggie media-video/ffmpeg +mp3 +x264 +faac && \
  CONFIG_PROTECT_MASK="/etc" dockemerge -v media-video/ffmpeg media-video/rtmpdump

RUN mkdir /app
WORKDIR /app

ADD Gemfile /app
ADD Gemfile.lock /app
RUN bundle install --jobs=3 --retry=3

ADD . /app/

ENV TZ Asia/Tokyo
ENV LANG en_US.UTF-8

CMD /app/start.sh
