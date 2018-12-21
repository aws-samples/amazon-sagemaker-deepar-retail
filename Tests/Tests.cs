// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DotStep.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Program.StateMachine;

namespace Tests
{
    [TestClass]
    public class StateMachines
    {
        [TestMethod]
        public async Task RetrainStateMachine()
        {
            var context = new Retrain.Context();

            var engine = new StateMachineEngine<Retrain, Retrain.Context>(context);

            await engine.Start();

            // if we made it this far, it worked.
            Assert.IsTrue(true);
        }
    }

    [TestClass]
    public class SyntheticData
    {
        [TestMethod]
        public async Task MakeSyntheticData()
        {
            var startDate = new DateTime(2013, 1, 1);
            var endDate = new DateTime(2017, 12, 31);

            var numberOfStores = 10;
            var numberOfItems = 50;

            var yearlyDemandIncrease = 0.015;

            var highestDemanddItem = 125;
            var lowestDemanddItem = 5;

            var totalDays = endDate.Subtract(startDate).TotalDays;


            var itemDemands = new Dictionary<int, int>();
            var storeModifiers = new Dictionary<int, decimal>();

            var weekModifiers = new Dictionary<DayOfWeek, decimal>
            {
                {DayOfWeek.Sunday, 0.16m},
                {DayOfWeek.Monday, 0.14m},
                {DayOfWeek.Tuesday, 0.12m},
                {DayOfWeek.Wednesday, 0.10m},
                {DayOfWeek.Thursday, 0.15m},
                {DayOfWeek.Friday, 0.17m},
                {DayOfWeek.Saturday, 0.18m}
            };

            for (var item = 1; item <= numberOfItems; item++)
            {
                var demand = new Random().Next(lowestDemanddItem, highestDemanddItem);
                itemDemands.Add(item, demand);
            }

            for (var store = 1; store <= numberOfStores; store++)
            {
                var modifier = new Random().Next(-20, 20);
                storeModifiers.Add(store, modifier);
            }

            using (var file = File.CreateText("train.csv"))
            {
                file.WriteLine("date,store,item,demand");
                for (var day = 0; day <= totalDays; day++)
                {
                    var date = startDate.AddDays(day);
                    var year = 1 + (date.Year - startDate.Year);

                    for (var store = 1; store <= numberOfStores; store++)
                    for (var item = 1; item <= numberOfItems; item++)
                    {
                        var startingDemand = itemDemands[item];
                        var demandIncrease = startingDemand * (year * yearlyDemandIncrease);
                        var demand = Convert.ToInt32(startingDemand + demandIncrease);
                        var modifier = storeModifiers[store] * 0.01m;
                        var storeModification = demand * modifier;
                        demand = Convert.ToInt32(demand + storeModification);

                        var dayOfWeekModifier = weekModifiers[date.DayOfWeek];

                        var dayOfWeekModification = demand * dayOfWeekModifier;
                        demand = Convert.ToInt32(demand + dayOfWeekModification);

                        await file.WriteLineAsync($"{date.Year}-{date.Month:D2}-{date.Day:D2},{store},{item},{demand}");
                    }
                }
            }
        }
    }
}