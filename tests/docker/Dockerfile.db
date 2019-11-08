ARG REVISION=HEAD
FROM comdb2test:${REVISION}

RUN mkdir /var/run/sshd && \
    echo 'root:bigsecret' | chpasswd && \
    sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    echo "StrictHostKeyChecking no" > /etc/ssh/ssh_config && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd && \
    echo "export VISIBLE=now" >> /etc/profile

ENV NOTVISIBLE "in users profile"

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
