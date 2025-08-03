This tracker is developed based on my personal need for a nutrition combined with the menstrual cycle tracker. 

If you want to read my story behind this project, please have a read at
https : // sekarlangit.substack.com/ p /i-automated-my-health-and-menstrual
(remove the spaces)

Now, let's cut the fluff and crack on with the tech stuff. 
💕How to use this tracker v1.0💕

 You log the nutrition facts of your foods (yes, you need to develop this habit) -> you log what you eat in a day -> you log the rough estimation of menstrual cycle days (you can adjust manually later) -> let Python and Sheets API automation populate the calculated values and update the cycle tracker for you.

💅I. Prepare your scaffolding💅 
You need 3 sheets. It doesn't matter whether you keep 3 separate files or 3 tabs in one file, you can always tell Python which is which later.
You can check at the .env.template file.
I have the variables regarding sheets for Food Log and Current Cycle because the way I structure my Google Sheets for those.

 1. Your Food Data. This is your master database. Whenever I buy something new, I record it in this log. If I don't know the nutrition sheet, say, from a restaurant, I upload the photos of the food and ask AI to estimate, which can skip this flow and record it directly on the next step.
 My GSheet for Food Data is structured like this:
 Sheet 1 is the Food Data sheet.
 Food | Alias | Unit | Per Unit | Kcal | Protein g | Carb g | Fat g
 Alias can be the English translation of a food name written in Mandarin, or the other names of a food, such as PB for peanut butter.
 Unit: g, ml, slice, and others based on what you know how the calories and macros are calculated.
 Per Unit: the denominator, e.g. 100 because the nutrition fact is usually calculated on 100g of weight.
Sheet 2 is my Scratchpad, where the Conversion column in the next step comes from.
I measure in raw weights as much as possible. 
So, for things like rice, legumes, etc which have different weight once cooked, I need a conversion factor to get the raw weight from the cooked one because when I weigh my cooked rice before eating, I need to know how many dry weight grams it equals to.
Therefore, my Scratchpad sheet can have multiple conversion factors for different foods and different cooking method.
Take rice again, I don't standardise how I cook rice (stereotypical, I know haha). I calculate the conversion factor for each batch. Although the factors sit in a certain range, it's good to be precise when precision is feasible.

 2. Your Food Log.
 Date | Food | Manual Input | Unit | Conversion | Value | Kcal | P | C | F
This is your WIEIAD (what I eat in a day) log.
The food column comes in a predictive text input based on the range imported from the Food Data - you use IMPORTRANGE for this and then data validation.
The Manual Input comes in a dropdown of Yes/No. If it's manual, you need to fill the Kcal | P | C | F data.
The Unit is populated with the INDEX MATCH formula from the imported range.
For food entries that require dry conversion, fill it in the Conversion column.
For food entries that calculate automatically, you only need to fill the value.
Examples below:
1. I eat 30g cooked rice with the known conversion factor of 0.25. 
So I only input: Rice (predictive input or alias), 0.25, and 30 for Food, Conversion, and Value, respectively.

3. Finally, your Cycle Tracker.
Cycle Day | Phase | Date | Calories | Protein (g) | Carbs (g) | Fat (g) | NEAT / Walk | Load-bearing | Bedtime | Mood / Notes
The automation will populate Calories, Protein, Carbs, and Fat column based on the Date column.
Therefore, the Date must be filled first before running the script.
The rest of the columns can be adjusted manually based on your cycle.

💅II. Test and run your script💅
Now is the Python time, the fun part!
You should be able to test and run it however way you like.
VS Code, a Jupyter notebook, or other suites come in handy.
The minimum_requirements.txt display the libraries I use.
Feel free to create your own classes and other functions to merge to the code you download.
My code is the baseline, the tracker is yours, so you get to play around with the script.

💅III. Schedule it💅
Take your automation further so you don't need to have your computer on to run the script.
I started with Mac Automator to trigger the flow locally, which is handy when I want to run the script after I record my breakfast or lunch.
But I explored a cron job, too. I use GitHub as it's free despite limited. 
In this repo, I've got the health_tracker.yml to run the script daily at night.
Since the free task quota on GitHub is limited, a daily run is enough for my use case. I keep my Automator in my computer for local triggers.

That's it! Improve the base script based on your use case.
Work with the AI, be bold in your code.
Reach out to me for any questions:
ushers_ceiba.86 [at] icloud [dot] com
or use the GitHub contact form.

Cheers,