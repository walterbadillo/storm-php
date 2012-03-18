Feature: Shell Component
  In order to use PHP as an implementation of a Storm shell component
  As a PHP developer
  I want to initialise the PHP component as the Shell client would expect

Background:
    Given a Storm configuration of "{'topology.workers':1}"
      And a topology context of "{'task->component':{'3':'n2','2':'n1','1':'__acker'},'taskid':1}"

Scenario: Bolt shell executes
    Given I create the "Storm_Component_Shell" component
    When I initialise the component
    Then I expect to send the process ID
      And I expect to receive the Storm configuration
      And I expect to receive the topology context
