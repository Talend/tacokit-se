#!/usr/bin/env php
<?php

if (PHP_SAPI !== 'cli') {
    echo 'bin/magento must be run as a CLI application';
    exit(1);
}

use Magento\Framework\App\Bootstrap;
include_once('/var/www/html/app/bootstrap.php');

$bootstrap = Bootstrap::create(BP, $_SERVER);
$objectManager = $bootstrap->getObjectManager();

//Set your Data
$name = 'TalendTest';
$email = '';
$endpoint = '';

// Code to check whether the Integration is already present or not
$integrationExists = $objectManager->get('Magento\Integration\Model\IntegrationFactory')->create()->load($name,'name')->getData();
if(empty($integrationExists)){
    $integrationData = array(
        'name' => $name,
        'email' => $email,
        'status' => '1',
        'endpoint' => $endpoint,
        'setup_type' => '0'
    );
    try{
        // Code to create Integration
        $integrationFactory = $objectManager->get('Magento\Integration\Model\IntegrationFactory')->create();
        $integration = $integrationFactory->setData($integrationData);
        $integration->save();
        $integrationId = $integration->getId();$consumerName = 'Integration' . $integrationId;


        // Code to create consumer
        $oauthService = $objectManager->get('Magento\Integration\Model\OauthService');
        $consumer = $oauthService->createConsumer(['name' => $consumerName]);
        $consumerId = $consumer->getId();
        $integration->setConsumerId($consumer->getId());
        $integration->save();


        // Code to grant permission
        $authrizeService = $objectManager->get('Magento\Integration\Model\AuthorizationService');
        $authrizeService->grantAllPermissions($integrationId);


        // Code to Activate and Authorize
        $token = $objectManager->get('Magento\Integration\Model\Oauth\Token');
        $uri = $token->createVerifierToken($consumerId);
        $token->setType('access');
        $token->save();
		
		echo 'token:'.$token->getToken();
    }catch(Exception $e){
        echo 'Error : '.$e->getMessage();
    }
}

