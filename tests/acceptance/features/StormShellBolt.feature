Feature: Shell Bolt
  In order to use PHP as an implementation of a Storm shell bolt
  As a PHP developer
  I want the PHP bolt to send and receive messages when processing tuples

Background:
    Given a tuple is waiting in the pipeline
      And the tuple has an ID of "1234"
      And the tuple has a value of "[]"

Scenario: StormShellBolt processes a tuple
    Given I execute the "MockShellBolt" bolt
    When the bolt processes the tuple "1234"
    Then I expect to send a sync command

Scenario: StormBasicBolt succeeds in processing a tuple
    Given I execute the "MockBasicBoltWithSuccess" bolt
    When the bolt succeeds in processing the tuple "1234"
    Then I expect to send a ack command
    And I expect to send a sync command

Scenario: StormBasicBolt fails to process a tuple
    Given I execute the "MockBasicBoltWithFailure" bolt
    When the bolt fails to process the tuple "1234"
    Then I expect to send a fail command
    And I expect to send a sync command

Scenario: StormBasicBolt throw an exception
    Given I execute the "MockBasicBoltWithException" bolt
    When the bolt triggers an exception while processing the tuple "1234"
    Then I expect to send a fail command
    And I expect to send a sync command
