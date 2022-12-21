ARG base_image
FROM ${base_image}
LABEL com.cisco.webex.image.authors="yejfeng@cisco.com"


ENV USER=wbxdba

ARG UID=10001
ARG GID=10001

RUN groupadd --system --gid $GID wbxgroup || true \
    && useradd -g wbxgroup -d /home/wbxdba -u $UID $USER || true \
    && id $USER \
     # install
    && yum install -y python3-pip \
    && yum install -y ansible \
    && yum install -y openssh-clients \
    && if test ! -d '/opt/logs/'; then mkdir -p -m 777 /opt/logs/; fi \
    && if test ! -d '/opt/repo/'; then mkdir -p -m 777 /opt/repo/; fi \
    # clean
    && rm -rf /resources || true \
    && yum clean all && rm -rf /var/cache/dnf \
    && find /usr/share/man/ -type f|xargs rm -fr \
    && find /usr/share/info/ -type f|xargs rm -fr \
    && if test ! -d '/var/webex/version/'; then mkdir -p /var/webex/version/; fi \
    && echo $(date +%Y%m%d) > /var/webex/version/build_date \
    && cat /var/webex/version/build_date


COPY --chown=$USER:wbxgroup requirements.txt /requirements.txt
COPY --chown=$USER:wbxgroup library/ /opt/ansible-dbpatch-library
RUN pip3 install -U pip setuptools \
    && pip3 install --no-cache-dir -r /requirements.txt
RUN chmod -R 777 /opt/ \
    && ls -l /opt/ansible-dbpatch-library \
    && pip3 list \
    && python3 --version

USER $UID
#CMD /bin/sh -c bash


