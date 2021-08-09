# -*- mode: ruby -*-
# vi: set ft=ruby :

unless system("sudo -n true 2> /dev/null")
    puts('Sudo is required, do a sudo true')
    exit
end

# to be able to modify tests outside of vagrant, we use nfs mount point, for more
# information refer https://www.vagrantup.com/docs/synced-folders/nfs
Vagrant.configure("2") do |config|
    config.vm.box = "generic/ubuntu2004"
    config.vm.synced_folder ".", "/vagrant", type: "nfs"

    $script = <<-SCRIPT
        ssh-keyscan localhost >> ~/.ssh/known_hosts
        ssh-keygen -N '' -f ~/.ssh/id_rsa
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: false

    $script = <<-SCRIPT
        export DEBIAN_FRONTEND="noninteractive"

        # optionally enable use of ng apt cacher on the host
        export NG_PROXY_URL="#{ENV['NG_PROXY_URL']}"
        if [ "$NG_PROXY_URL" != "" ]; then
            echo "Enabling Apt Proxy ($NG_PROXY_URL) ..."
            echo 'Acquire::http { Proxy "$NG_PROXY_URL"; };' > /etc/apt/apt.conf.d/02proxy
        fi

        echo "deb http://apt.postgresql.org/pub/repos/apt/ focal-pgdg main" > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
        add-apt-repository -y ppa:deadsnakes/ppa

        apt-get update
        apt-get install -y build-essential libsnappy-dev postgresql-common

        # no point creating the default cluster as its not used for tests
        sed -i "s/^#start_conf.*/start_conf='manual'/g" /etc/postgresql-common/createcluster.conf
        sed -i "s/^#create_main_cluster.*/create_main_cluster=false/g" /etc/postgresql-common/createcluster.conf

        apt-get install -y python3.6 python3.6-dev python3.6-venv python3.7 python3.7-dev python3.7-venv python3.8 python3.8-dev python3.8-venv python3.9 python3.9-dev python3.9-venv
        apt-get install -y postgresql-9.6 postgresql-server-dev-9.6 postgresql-10 postgresql-server-dev-10 postgresql-11 postgresql-server-dev-11 postgresql-12 postgresql-server-dev-12

        username="$(< /dev/urandom tr -dc a-z | head -c${1:-32};echo;)"
        password=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-32};echo;)
        useradd -m -U $username
        echo "$username:$password" > /home/vagrant/pghoard-test-sftp-user
        echo "$username:$password" | chpasswd

        mkdir -p /home/$username/.ssh
        cat /home/vagrant/.ssh/id_rsa.pub >> /home/$username/.ssh/authorized_keys
        chown -R $username: /home/$username/.ssh
        chmod -R go-rwx /home/$username/.ssh

        sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config

        # later versions have the Port 22 config disabled (cos its the default), so need to
        # explicitly enable it to avoid ssh only using port 23.
        echo "Port 22" >> /etc/ssh/sshd_config

        # this is for sftp testing
        echo "Port 23" >> /etc/ssh/sshd_config
        echo "Match LocalPort 22" >> /etc/ssh/sshd_config
        echo "	DenyUsers $username" >> /etc/ssh/sshd_config
        systemctl reload ssh
    SCRIPT

    config.vm.provision "shell", inline: $script, privileged: true

    $script = <<-SCRIPT
        versions=(3.6 3.7 3.8 3.9)
        for version in "${versions[@]}"; do
            python${version} -m venv venv${version}
            source ~/venv${version}/bin/activate
            pip install --upgrade pip
            pip install -r /vagrant/requirements.txt
            pip install --upgrade -r /vagrant/requirements.dev.txt
        done
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: false
end
